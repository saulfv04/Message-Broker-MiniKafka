// broker.c - Broker que maneja múltiples conexiones de productores y consumidores mediante hilos

// =======================
//  INCLUDES Y DEFINES
// =======================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <limits.h>
#include <errno.h>
#include <signal.h>
#include <sys/select.h>

#define _XOPEN_SOURCE 700
#define _POSIX_C_SOURCE 200809L
#define PUERTO 4444
#define MAX_CONEXIONES 10
#define LONGITUD_MAXIMA_MENSAJE 256
#define LONGITUD_MAXIMA_MENSAJES 100
#define NOMBRE_MEMORIA "/memoria_cola_mensajes"
#define MAX_CONSUMIDORES 32
#define MAX_GRUPOS 16
#define MAX_CONSUMIDORES_POR_GRUPO 16
#define LONGITUD_NOMBRE_GRUPO 32
#define MAX_CLIENTES_POR_HILO 32
#define BUFFER_PERSISTENCIA 128

// =======================
//  ESTRUCTURAS DE DATOS
// =======================
typedef struct {
    int id;
    char contenido[LONGITUD_MAXIMA_MENSAJE];
    time_t timestamp;
} Mensaje;

typedef struct {
    Mensaje mensajes[LONGITUD_MAXIMA_MENSAJES];
    int frente;
    int final;
    int base_offset; 
    // Offset absoluto del mensaje en 'frente'
    //El base offset se incrementa al consumir mensajes
    // y se utiliza para determinar si un mensaje puede ser eliminado
    //por ejemplo, si el offset de un grupo es menor que el base_offset
    // significa que el grupo no ha leído el mensaje en 'frente'
    // y por lo tanto el mensaje puede ser eliminado
    

} ColaMensajes;


typedef struct {
    char nombre[LONGITUD_NOMBRE_GRUPO];
    int consumidores[MAX_CONSUMIDORES_POR_GRUPO]; // IDs de consumidores activos
    int num_consumidores;
    int offset; // Offset de grupo (absoluto)
    int turno;  // Índice del consumidor al que le toca leer
} GrupoConsumidor;

typedef struct {
    int sockets[MAX_CLIENTES_POR_HILO];
    char tipo_cliente[MAX_CLIENTES_POR_HILO]; // 'P' o 'C'
    int estado_cliente[MAX_CLIENTES_POR_HILO]; // 0: handshake, 1: listo
    int grupo_idx_cliente[MAX_CLIENTES_POR_HILO];
    int id_consumidor_cliente[MAX_CLIENTES_POR_HILO];
    int cantidad_sockets;
    pthread_mutex_t mutex;
} PoolSockets;

// =======================
//  VARIABLES GLOBALES
// =======================
ColaMensajes *cola;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int ids_libres[MAX_CONSUMIDORES];      // Pila de IDs libres
int tope_libres = 0;                   // Tope de la pila

GrupoConsumidor grupos[MAX_GRUPOS];
int num_grupos = 0;
pthread_mutex_t mutex_grupos = PTHREAD_MUTEX_INITIALIZER;

// Pool de hilos
int tamano_pool_hilos = 8;      // Valor por defecto, puedes cambiarlo por argumento
pthread_t *pool_hilos = NULL;
PoolSockets *pool_sockets_array = NULL;

// Sincronización adicional
pthread_mutex_t mutex_limpieza = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_limpieza = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex_mensajes = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_mensajes = PTHREAD_COND_INITIALIZER;

// Doble buffer para persistencia
Mensaje buffer_a[BUFFER_PERSISTENCIA];
Mensaje buffer_b[BUFFER_PERSISTENCIA];
Mensaje *buffer_escritura = buffer_a;
Mensaje *buffer_lectura = buffer_b;
int idx_escritura = 0, idx_lectura = 0;
pthread_mutex_t mutex_buffer = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_buffer = PTHREAD_COND_INITIALIZER;

// Variable global para terminar
volatile sig_atomic_t terminar = 0;

// Manejador de señales
void handler(int sig) {
    terminar = 1;
}

// =======================
//  PROTOTIPOS
// =======================
void limpiar_cola_si_es_posible(ColaMensajes *c);

// =======================
//  FUNCIONES DE COLA DE MENSAJES
// =======================
int esta_llena(ColaMensajes *c) {
    return (c->final + 1) % LONGITUD_MAXIMA_MENSAJES == c->frente;
}

int esta_vacia(ColaMensajes *c) {
    return c->frente == c->final;
}

int insertar_mensaje(ColaMensajes *c, Mensaje m) {
    if (esta_llena(c)) return -1;
    c->mensajes[c->final] = m;
    c->final = (c->final + 1) % LONGITUD_MAXIMA_MENSAJES;
    return 0;
}

int consumir_mensaje(ColaMensajes *c, Mensaje *m) {
    if (esta_vacia(c)) return -1;
    *m = c->mensajes[c->frente];
    c->frente = (c->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
    c->base_offset++;
    return 0;
}


// =======================
//  FUNCIONES DE GRUPOS Y OFFSETS
// =======================

// Función para obtener el offset mínimo de todos los grupos
// Esto se utiliza para determinar si un mensaje puede ser eliminado de la cola
// de mensajes. Si el offset mínimo es mayor que el base_offset de la cola,
// significa que todos los grupos han leído el mensaje en 'frente' y se puede
int obtener_offset_minimo_grupos() {
    int min_offset = INT_MAX;
    pthread_mutex_lock(&mutex_grupos);
    for (int i = 0; i < num_grupos; ++i) {
        if (grupos[i].offset < min_offset) {
            min_offset = grupos[i].offset;
        }
    }
    //tiene complejidad O(n)
    //se podria optimizar si se mantiene un offset mínimo global
    //pero eso complicaria la logica de los grupos
    //y no es necesario para el funcionamiento
    //o con un hashmap
    //se puede hacer un hashmap de offsets por grupo
    //pero eso complicaria la logica de los grupos
    pthread_mutex_unlock(&mutex_grupos);
    return min_offset;
}

//esta funcion se encarga de insertar un mensaje en la cola de mensajes
//y de eliminar mensajes antiguos si es necesario
//si la cola de mensajes esta llena y el offset minimo de los grupos
//es menor o igual al base_offset de la cola, significa que
//no se puede eliminar el mensaje mas antiguo

int insertar_mensaje_con_retencion(ColaMensajes *c, Mensaje m) {
    if (esta_llena(c)) {
        int min_offset = obtener_offset_minimo_grupos();
        if (min_offset > c->base_offset) {
            // Todos los grupos ya leyeron el mensaje más antiguo, avanza el frente
    c->frente = (c->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
    c->base_offset++;
    // Cuando "eliminas" un mensaje de la cola (avanzando frente y base_offset), simplemente dejas de considerar ese espacio como válido, pero no hay memoria dinámica que liberar.
    // El espacio se reutiliza automáticamente cuando insertas un nuevo mensaje (por el manejo circular de la cola).
} else {
            // No se puede eliminar el mensaje más antiguo
            return -1;
        }
    }
    c->mensajes[c->final] = m;
    c->final = (c->final + 1) % LONGITUD_MAXIMA_MENSAJES;
    return 0;
}

void limpiar_cola_si_es_posible(ColaMensajes *c) {
    int min_offset = obtener_offset_minimo_grupos();
    while (c->frente != c->final && min_offset > c->base_offset) {
        c->frente = (c->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
        c->base_offset++;
    }
}

// =======================
//  FUNCIONES DE DOBLE BUFFER PARA PERSISTENCIA
// =======================
void agregar_a_persistencia(Mensaje m) {
    pthread_mutex_lock(&mutex_buffer);
    buffer_escritura[idx_escritura++] = m;
    if (idx_escritura == BUFFER_PERSISTENCIA) {
        // Swap buffers
        Mensaje *tmp = buffer_escritura;
        buffer_escritura = buffer_lectura;
        buffer_lectura = tmp;
        idx_lectura = idx_escritura;
        idx_escritura = 0;
        pthread_cond_signal(&cond_buffer);
    }
    pthread_mutex_unlock(&mutex_buffer);
}

void flush_persistencia() {
    pthread_mutex_lock(&mutex_buffer);
    if (idx_escritura > 0) {
        Mensaje *tmp = buffer_escritura;
        buffer_escritura = buffer_lectura;
        buffer_lectura = tmp;
        idx_lectura = idx_escritura;
        idx_escritura = 0;
        pthread_cond_signal(&cond_buffer);
    }
    pthread_mutex_unlock(&mutex_buffer);
}

// =======================
//  HILOS AUXILIARES (PERSISTENCIA Y LIMPIEZA)
// =======================
void* hilo_persistencia_func(void* arg) {
    while (!terminar) {
        pthread_mutex_lock(&mutex_buffer);
        while (idx_lectura == 0 && !terminar)
            pthread_cond_wait(&cond_buffer, &mutex_buffer);
        int cantidad = idx_lectura;
        idx_lectura = 0;
        pthread_mutex_unlock(&mutex_buffer);

        for (int i = 0; i < cantidad; ++i) {
            Mensaje *m = &buffer_lectura[i];
            FILE *f = fopen("log_mensajes.txt", "a");
            if (f) {
                fprintf(f, "ID: %d | %s | %ld\n", m->id, m->contenido, m->timestamp);
                fclose(f);
            }
        }
    }
    return NULL;
}

// Hilo de limpieza que se activa cuando hay mensajes en la cola
// y el offset mínimo de los grupos es mayor que el base_offset
// Esto permite liberar espacio en la cola de mensajes
// y evitar que se llene innecesariamente

void* hilo_limpieza_func(void* arg) {
    while (!terminar) {
        pthread_mutex_lock(&mutex_limpieza);
        pthread_cond_wait(&cond_limpieza, &mutex_limpieza);
        pthread_mutex_unlock(&mutex_limpieza);

        pthread_mutex_lock(&mutex);
        limpiar_cola_si_es_posible(cola);
        pthread_mutex_unlock(&mutex);
    }
    return NULL;
}

// =======================
//  WORKER MULTIPLEXADO
// =======================
void* trabajador(void* arg) {
    int thread_index = *(int*)arg;
    free(arg);

    PoolSockets *pool = &pool_sockets_array[thread_index];

    fd_set read_fds;
    int maxfd;

    while (!terminar) {
        FD_ZERO(&read_fds);
        maxfd = -1;

        pthread_mutex_lock(&pool->mutex);
        for (int i = 0; i < pool->cantidad_sockets; ++i) {
            FD_SET(pool->sockets[i], &read_fds);
            if (pool->sockets[i] > maxfd) maxfd = pool->sockets[i];
        }
        pthread_mutex_unlock(&pool->mutex);

        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 10000; // 10 ms

        int ready = select(maxfd + 1, &read_fds, NULL, NULL, (maxfd == -1) ? &timeout : NULL);
        if (ready < 0) continue;

        pthread_mutex_lock(&pool->mutex);
        for (int i = 0; i < pool->cantidad_sockets; ++i) {
            int sock = pool->sockets[i];
            if (!FD_ISSET(sock, &read_fds)) continue;

            // Handshake inicial
            if (pool->estado_cliente[i] == 0) {
                char tipo;
                int r = recv(sock, &tipo, 1, 0);
                if (r <= 0) goto desconectar;
                pool->tipo_cliente[i] = tipo;
                pool->estado_cliente[i] = 1;

                if (tipo == 'C') {
                    char nombre_grupo[LONGITUD_NOMBRE_GRUPO];
                    r = recv(sock, nombre_grupo, LONGITUD_NOMBRE_GRUPO, 0);
                    if (r <= 0) goto desconectar;
                    int grupo_idx = -1;
                    pthread_mutex_lock(&mutex_grupos);
                    for (int g = 0; g < num_grupos; ++g)
                        if (strcmp(grupos[g].nombre, nombre_grupo) == 0) grupo_idx = g;
                    if (grupo_idx == -1 && num_grupos < MAX_GRUPOS) {
                        grupo_idx = num_grupos++;
                        strncpy(grupos[grupo_idx].nombre, nombre_grupo, LONGITUD_NOMBRE_GRUPO);
                        grupos[grupo_idx].num_consumidores = 0;
                        grupos[grupo_idx].offset = cola->base_offset;
                        grupos[grupo_idx].turno = 0;
                    }
                    if (grupo_idx == -1 || grupos[grupo_idx].num_consumidores >= MAX_CONSUMIDORES_POR_GRUPO) {
                        send(sock, "GRUPO_NO_DISPONIBLE", 20, 0);
                        pthread_mutex_unlock(&mutex_grupos);
                        goto desconectar;
                    }
                    int cid = grupos[grupo_idx].num_consumidores++;
                    grupos[grupo_idx].consumidores[cid] = cid;
                    pool->grupo_idx_cliente[i] = grupo_idx;
                    pool->id_consumidor_cliente[i] = cid;
                    send(sock, &cid, sizeof(int), 0);
                    pthread_mutex_unlock(&mutex_grupos);
                }
                continue;
            }

            // Productor
            if (pool->tipo_cliente[i] == 'P') {
                Mensaje m;
                int r = recv(sock, &m, sizeof(Mensaje), 0);
                if (r <= 0) goto desconectar;
                pthread_mutex_lock(&mutex);
                int resultado_insercion = insertar_mensaje_con_retencion(cola, m);
                pthread_mutex_unlock(&mutex);
                send(sock, (resultado_insercion == 0) ? "OK" : "LLENO", 6, 0);

                // Persistencia (doble buffer)
                agregar_a_persistencia(m);

                pthread_mutex_lock(&mutex_mensajes);
                pthread_cond_broadcast(&cond_mensajes);
                pthread_mutex_unlock(&mutex_mensajes);
                continue;
            }

            // Consumidor
            if (pool->tipo_cliente[i] == 'C') {
                char peticion;
                int r1 = recv(sock, &peticion, 1, 0);
                if (r1 <= 0) goto desconectar;

                while (peticion != 'R') {
                    r1 = recv(sock, &peticion, 1, 0);
                    if (r1 <= 0) goto desconectar;
                }

                int id_recv;
                int r2 = recv(sock, &id_recv, sizeof(int), 0);
                if (r2 != sizeof(int)) goto desconectar;

                int grupo_idx = pool->grupo_idx_cliente[i];
                pthread_mutex_lock(&mutex_grupos);
                GrupoConsumidor *grupo = &grupos[grupo_idx];
                int es_turno = (grupo->consumidores[grupo->turno] == pool->id_consumidor_cliente[i]);
                pthread_mutex_unlock(&mutex_grupos);

                // 1. Lock solo para la cola
                pthread_mutex_lock(&mutex);
                int cantidad_en_cola = (cola->final - cola->frente + LONGITUD_MAXIMA_MENSAJES) % LONGITUD_MAXIMA_MENSAJES;
                int hay_mensaje = grupo->offset >= cola->base_offset && grupo->offset < cola->base_offset + cantidad_en_cola;
                int idxmsg = -1;
                Mensaje m;
                if (es_turno && hay_mensaje) {
                    idxmsg = (cola->frente + (grupo->offset - cola->base_offset)) % LONGITUD_MAXIMA_MENSAJES;
                    m = cola->mensajes[idxmsg];
                }
                pthread_mutex_unlock(&mutex);

                // 2. Lock solo para el grupo (si hay mensaje)
                if (es_turno && hay_mensaje) {
                    send(sock, &m, sizeof(Mensaje), 0);

                    pthread_mutex_lock(&mutex_grupos);
                    grupo->offset++;
                    grupo->turno = (grupo->turno + 1) % grupo->num_consumidores;
                    pthread_mutex_unlock(&mutex_grupos);

                    pthread_mutex_lock(&mutex_limpieza);
                    pthread_cond_signal(&cond_limpieza);
                    pthread_mutex_unlock(&mutex_limpieza);
                } else {
                    Mensaje vacio = {.id = -1};
                    strcpy(vacio.contenido, "VACIO");
                    vacio.timestamp = 0;
                    send(sock, &vacio, sizeof(Mensaje), 0);
                }
                continue;
            }

        desconectar:
            printf("[BROKER] Cliente desconectado: socket=%d\n", sock);
            // Si era consumidor, eliminar del grupo y liberar ID
            if (pool->tipo_cliente[i] == 'C') {
                int grupo_idx = pool->grupo_idx_cliente[i];
                int cid = pool->id_consumidor_cliente[i];
                if (grupo_idx >= 0 && cid >= 0) {
                    pthread_mutex_lock(&mutex_grupos);
                    GrupoConsumidor *grupo = &grupos[grupo_idx];
                    // Elimina el consumidor del grupo
                    for (int j = 0; j < grupo->num_consumidores; ++j) {
                        if (grupo->consumidores[j] == cid) {
                            grupo->consumidores[j] = grupo->consumidores[--grupo->num_consumidores];
                            break;
                        }
                    }
                    pthread_mutex_unlock(&mutex_grupos);
                    // Libera el ID
                    pthread_mutex_lock(&mutex);
                    ids_libres[tope_libres++] = cid;
                    pthread_mutex_unlock(&mutex);
                }
            }
            close(sock);
            pool->sockets[i] = pool->sockets[--pool->cantidad_sockets];
            pool->tipo_cliente[i] = pool->tipo_cliente[pool->cantidad_sockets];
            pool->estado_cliente[i] = pool->estado_cliente[pool->cantidad_sockets];
            pool->grupo_idx_cliente[i] = pool->grupo_idx_cliente[pool->cantidad_sockets];
            pool->id_consumidor_cliente[i] = pool->id_consumidor_cliente[pool->cantidad_sockets];
            --i;
        }
        pthread_mutex_unlock(&pool->mutex);
    }
    return NULL;
}

// =======================
//  MAIN
// =======================
int main(int argc, char *argv[]) {
    signal(SIGINT, handler);
    signal(SIGTERM, handler);

    // Permitir configurar tamaños por argumentos
    if (argc >= 2) tamano_pool_hilos = atoi(argv[1]);

    // Reserva dinámica de memoria para el pool de hilos
    pool_hilos = malloc(sizeof(pthread_t) * tamano_pool_hilos);

    // Configurar memoria compartida
    int shm_fd = shm_open(NOMBRE_MEMORIA, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open");
        exit(EXIT_FAILURE);
    }
    if (ftruncate(shm_fd, sizeof(ColaMensajes)) == -1) {
        perror("ftruncate");
        exit(EXIT_FAILURE);
    }
    cola = mmap(NULL, sizeof(ColaMensajes), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (cola == MAP_FAILED) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }
    cola->frente = cola->final = 0;
    cola->base_offset = 0;

    // Inicializar la pila de IDs libres
    for (int i = 0; i < MAX_CONSUMIDORES; ++i) {
        ids_libres[i] = MAX_CONSUMIDORES - 1 - i; // IDs del mayor al menor para pop eficiente
    }
    tope_libres = MAX_CONSUMIDORES;

    // Crear hilo de persistencia
    pthread_t hilo_persistencia;
    pthread_create(&hilo_persistencia, NULL, hilo_persistencia_func, NULL);

    // Crear hilo de limpieza
    pthread_t hilo_limpieza;
    pthread_create(&hilo_limpieza, NULL, hilo_limpieza_func, NULL);

    // Crear socket del servidor
    int servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (servidor == -1) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(servidor, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PUERTO);

    if (bind(servidor, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        close(servidor);
        exit(EXIT_FAILURE);
    }
    if (listen(servidor, MAX_CONEXIONES) == -1) {
        perror("listen");
        close(servidor);
        exit(EXIT_FAILURE);
    }
    printf("[BROKER] Escuchando en el puerto %d...\n", PUERTO);

    // Crear pool de hilos dinámico
    pool_sockets_array = calloc(tamano_pool_hilos, sizeof(PoolSockets));
    for (int i = 0; i < tamano_pool_hilos; ++i) {
        pthread_mutex_init(&pool_sockets_array[i].mutex, NULL);
        pool_sockets_array[i].cantidad_sockets = 0;
        int *idx = malloc(sizeof(int));
        *idx = i;
        pthread_create(&pool_hilos[i], NULL, trabajador, idx);
    }

    while (!terminar) {
        int socket_cliente = accept(servidor, NULL, NULL);
        if (terminar) break;
        if (socket_cliente == -1) {
            if (terminar) break;
            perror("accept");
            continue;
        }
        printf("[BROKER] Nueva conexión aceptada, socket: %d\n", socket_cliente);
        // Busca el hilo con menos sockets
        int min_idx = 0, min_cant = pool_sockets_array[0].cantidad_sockets;
        for (int i = 1; i < tamano_pool_hilos; ++i) {
            if (pool_sockets_array[i].cantidad_sockets < min_cant) {
                min_cant = pool_sockets_array[i].cantidad_sockets;
                min_idx = i;
            }
        }
        PoolSockets *pool = &pool_sockets_array[min_idx];
        pthread_mutex_lock(&pool->mutex);
        if (pool->cantidad_sockets >= MAX_CLIENTES_POR_HILO) {
            const char* msg = "SERVIDOR_OCUPADO";
            send(socket_cliente, msg, strlen(msg), 0);
            close(socket_cliente);
        } else {
            pool->sockets[pool->cantidad_sockets] = socket_cliente;
            pool->tipo_cliente[pool->cantidad_sockets] = 0;
            pool->estado_cliente[pool->cantidad_sockets] = 0;
            pool->grupo_idx_cliente[pool->cantidad_sockets] = -1;
            pool->id_consumidor_cliente[pool->cantidad_sockets] = -1;
            pool->cantidad_sockets++;
            printf("[BROKER] Nueva conexión aceptada: socket=%d asignado al pool/hilo %d\n", socket_cliente, min_idx);
        }
        pthread_mutex_unlock(&pool->mutex);
    }

    // Notificar a los hilos bloqueados
    pthread_mutex_lock(&mutex_buffer);
    pthread_cond_broadcast(&cond_buffer);
    pthread_mutex_unlock(&mutex_buffer);

    pthread_mutex_lock(&mutex_limpieza);
    pthread_cond_broadcast(&cond_limpieza);
    pthread_mutex_unlock(&mutex_limpieza);

    // Esperar a que terminen los hilos del pool y auxiliares
    for (int i = 0; i < tamano_pool_hilos; ++i)
        pthread_join(pool_hilos[i], NULL);
    pthread_join(hilo_persistencia, NULL);
    pthread_join(hilo_limpieza, NULL);

    // Liberar memoria dinámica
    free(pool_hilos);

    close(servidor);
    munmap(cola, sizeof(ColaMensajes));
    close(shm_fd);
    shm_unlink(NOMBRE_MEMORIA);
    
    // Asegurar que todo lo pendiente se escriba
    flush_persistencia();
    
    return 0;
    //probar con gcc broker.c -o broker -lpthread
}
