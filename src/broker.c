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
#include <semaphore.h>

#define _XOPEN_SOURCE 700
#define _POSIX_C_SOURCE 200809L
#define PUERTO 4444
#define LONGITUD_MAXIMA_MENSAJE 256
#define LONGITUD_MAXIMA_MENSAJES 100
#define NOMBRE_MEMORIA "/memoria_cola_mensajes"

#define MAX_CONEXIONES 10
#define MAX_CONSUMIDORES 32
#define MAX_GRUPOS 16
#define MAX_CONSUMIDORES_POR_GRUPO 16
#define LONGITUD_NOMBRE_GRUPO 32
#define MAX_CLIENTES_POR_HILO 32
#define BUFFER_PERSISTENCIA 128
#define BUFFER_LOG_CONSUMO 128

// --- Agrega esto después de los includes ---
void sumar_milisegundos(struct timespec *ts, int ms) {
    ts->tv_sec += ms / 1000;
    ts->tv_nsec += (ms % 1000) * 1000000;
    if (ts->tv_nsec >= 1000000000) {
        ts->tv_sec += 1;
        ts->tv_nsec -= 1000000000;
    }
}

// =======================
//  ESTRUCTURAS DE DATOS
// =======================
typedef struct {
    int id;
    char contenido[LONGITUD_MAXIMA_MENSAJE];
    time_t timestamp;
    int productor_socket; // Nuevo campo: socket del productor
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

typedef struct ConsumidorNodo {
    int id;
    struct ConsumidorNodo* siguiente;
} ConsumidorNodo;

typedef struct {
    char nombre[LONGITUD_NOMBRE_GRUPO];
    ConsumidorNodo* consumidores; // <-- Ahora es una lista enlazada
    int num_consumidores;
    int offset; // Offset de grupo (absoluto)
    int turno;  // Índice del consumidor al que le toca leer
} GrupoConsumidor;

typedef struct {
    int *sockets;
    char *tipo_cliente; // 'P' o 'C'
    int *estado_cliente; // 0: handshake, 1: listo
    int *grupo_idx_cliente;
    int *id_consumidor_cliente;
    int cantidad_sockets;
    int capacidad; // capacidad actual del array
    pthread_mutex_t mutex;
} PoolSockets;

typedef struct {
    int mensaje_id;
    int productor_socket;
    char grupo[LONGITUD_NOMBRE_GRUPO];
    int consumidor_id;
    time_t timestamp;
} EventoConsumo;

// =======================
//  VARIABLES GLOBALES
// =======================
ColaMensajes *cola;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

// Usa un contador global para IDs de consumidores
static int next_cid = 0;

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

EventoConsumo buffer_log_a[BUFFER_LOG_CONSUMO];
EventoConsumo buffer_log_b[BUFFER_LOG_CONSUMO];
EventoConsumo *buffer_log_escritura = buffer_log_a;
EventoConsumo *buffer_log_lectura = buffer_log_b;
int idx_log_escritura = 0, idx_log_lectura = 0;
pthread_mutex_t mutex_log = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_log = PTHREAD_COND_INITIALIZER;

// Variable global para terminar
volatile sig_atomic_t terminar = 0;

// Declarar 'servidor' como variable global para acceso en el handler
int servidor = -1;

// Manejador de señales
void handler(int sig) {
    printf("Recibida señal %d, terminando...\n", sig);
    terminar = 1;
    if (servidor != -1) close(servidor);

    // Despertar hilos auxiliares inmediatamente
    pthread_cond_broadcast(&cond_buffer);
    pthread_cond_broadcast(&cond_limpieza);
    pthread_cond_broadcast(&cond_log);
}

// =======================
//  PROTOTIPOS
// =======================
void limpiar_cola_si_es_posible(ColaMensajes *c);
void liberar_consumidores(ConsumidorNodo* cabeza);

// =======================
//  VARIABLES GLOBALES (agrega después de los mutex)
// =======================
sem_t sem_espacios_libres;
sem_t sem_mensajes_disponibles;


// Variable global para IDs de mensajes y su mutex
static int next_msg_id = 1;
pthread_mutex_t mutex_id = PTHREAD_MUTEX_INITIALIZER;

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

void agregar_a_log_consumo(EventoConsumo e) {
    pthread_mutex_lock(&mutex_log);
    buffer_log_escritura[idx_log_escritura++] = e;
    if (idx_log_escritura == BUFFER_LOG_CONSUMO) {
        EventoConsumo *tmp = buffer_log_escritura;
        buffer_log_escritura = buffer_log_lectura;
        buffer_log_lectura = tmp;
        idx_log_lectura = idx_log_escritura;
        idx_log_escritura = 0;
        pthread_cond_signal(&cond_log);
    }
    pthread_mutex_unlock(&mutex_log);
}

// =======================
//  HILOS AUXILIARES (PERSISTENCIA Y LIMPIEZA)
// =======================
void* hilo_persistencia_func(void* arg) {
    FILE *f = fopen("persistencia_mensajes.txt", "a");
    if (!f) return NULL;
    while (!terminar) {
        pthread_mutex_lock(&mutex_buffer);
        while (idx_lectura == 0 && !terminar)
            pthread_cond_wait(&cond_buffer, &mutex_buffer);
        int cantidad = idx_lectura;
        idx_lectura = 0;
        pthread_mutex_unlock(&mutex_buffer);

        for (int i = 0; i < cantidad; ++i) {
            Mensaje *m = &buffer_lectura[i];
            fprintf(f, "ID: %d | %s | %ld\n", m->id, m->contenido, m->timestamp);
        }
        fflush(f); // Asegura que los datos se escriban en disco
    }
    fclose(f);
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

void* hilo_log_consumo_func(void* arg) {
    FILE *f = fopen("log_consumo.txt", "a");
    if (!f) return NULL;
    while (!terminar) {
        pthread_mutex_lock(&mutex_log);
        while (idx_log_lectura == 0 && !terminar)
            pthread_cond_wait(&cond_log, &mutex_log);
        int cantidad = idx_log_lectura;
        idx_log_lectura = 0;
        pthread_mutex_unlock(&mutex_log);

        for (int i = 0; i < cantidad; ++i) {
            EventoConsumo *e = &buffer_log_lectura[i];
            fprintf(f, "MensajeID: %d | ProductorSocket: %d | Grupo: %s | ConsumidorID: %d | Timestamp: %ld\n",
                e->mensaje_id, e->productor_socket, e->grupo, e->consumidor_id, e->timestamp);
        }
        fflush(f);
    }
    fclose(f);
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
            if (terminar) break; // <-- Agrega esto
            int sock = pool->sockets[i];
            FD_SET(sock, &read_fds);
            if (pool->sockets[i] > maxfd) maxfd = pool->sockets[i];
            if (terminar) break;
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
                // Agrega select() antes de recv()
                fd_set fds;
                struct timeval tv;
                FD_ZERO(&fds);
                FD_SET(sock, &fds);
                tv.tv_sec = 0;
                tv.tv_usec = 200000; // 200 ms

                int ready = select(sock + 1, &fds, NULL, NULL, &tv);
                if (ready < 0) continue;
                if (ready == 0) continue; // Timeout, revisa terminar en el while

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
                        grupos[grupo_idx].consumidores = NULL; // Inicializa la lista
                        grupos[grupo_idx].offset = cola->base_offset;
                        grupos[grupo_idx].turno = 0;
                    }
                    if (grupo_idx == -1) {
                        send(sock, "GRUPO_NO_DISPONIBLE", 20, 0);
                        pthread_mutex_unlock(&mutex_grupos);
                        goto desconectar;
                    }
                    // Genera un nuevo ID de consumidor (puedes usar un contador global o similar)
                    int cid = next_cid++;

                    // Agrega el consumidor a la lista enlazada
                    ConsumidorNodo* nuevo = malloc(sizeof(ConsumidorNodo));
                    nuevo->id = cid;
                    nuevo->siguiente = grupos[grupo_idx].consumidores;
                    grupos[grupo_idx].consumidores = nuevo;
                    grupos[grupo_idx].num_consumidores++;

                    pool->grupo_idx_cliente[i] = grupo_idx;
                    pool->id_consumidor_cliente[i] = cid;
                    send(sock, &cid, sizeof(int), 0);
                    pthread_mutex_unlock(&mutex_grupos);
                    continue;
                }
                continue;
            }

            // Productor
            if (pool->tipo_cliente[i] == 'P') {
                Mensaje m;
                fd_set fds;
                struct timeval tv;
                FD_ZERO(&fds);
                FD_SET(sock, &fds);
                tv.tv_sec = 0;
                tv.tv_usec = 200000; // 200 ms

                int ready = select(sock + 1, &fds, NULL, NULL, &tv);
                if (ready < 0) continue;
                if (ready == 0) continue; // Timeout, revisa terminar en el while

                // Ahora sí, recv()
                int r = recv(sock, &m, sizeof(Mensaje), 0);
                if (r <= 0) goto desconectar;

                // Guarda el socket del productor
                m.productor_socket = sock;

                pthread_mutex_lock(&mutex_id);
                m.id = next_msg_id++;
                pthread_mutex_unlock(&mutex_id);

                // Espera espacio libre en la cola
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                sumar_milisegundos(&ts, 200); // 200 ms

                while (!terminar && sem_timedwait(&sem_espacios_libres, &ts) == -1 && errno == ETIMEDOUT) {
                    clock_gettime(CLOCK_REALTIME, &ts);
                    sumar_milisegundos(&ts, 200);
                }
                if (terminar) break;

                pthread_mutex_lock(&mutex);
                int resultado_insercion = insertar_mensaje_con_retencion(cola, m);
                pthread_mutex_unlock(&mutex);

                // Señala que hay un mensaje disponible
                sem_post(&sem_mensajes_disponibles);

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
                // Espera petición del consumidor
                fd_set fds;
                struct timeval tv;
                FD_ZERO(&fds);
                FD_SET(sock, &fds);
                tv.tv_sec = 0;
                tv.tv_usec = 200000; // 200 ms

                int ready = select(sock + 1, &fds, NULL, NULL, &tv);
                if (ready < 0) continue;
                if (ready == 0) continue; // Timeout, revisa terminar en el while

                char peticion;
                int r1 = recv(sock, &peticion, 1, 0);
                if (r1 <= 0) goto desconectar;

                while (peticion != 'R') {
                    FD_ZERO(&fds);
                    FD_SET(sock, &fds);
                    tv.tv_sec = 0;
                    tv.tv_usec = 200000;
                    ready = select(sock + 1, &fds, NULL, NULL, &tv);
                    if (ready < 0) continue;
                    if (ready == 0) continue;
                    r1 = recv(sock, &peticion, 1, 0);
                    if (r1 <= 0) goto desconectar;
                }

                // Espera el id_recv también con select()
                FD_ZERO(&fds);
                FD_SET(sock, &fds);
                tv.tv_sec = 0;
                tv.tv_usec = 200000;
                ready = select(sock + 1, &fds, NULL, NULL, &tv);
                if (ready < 0) continue;
                if (ready == 0) continue;

                int id_recv;
                int r2 = recv(sock, &id_recv, sizeof(int), 0);
                if (r2 != sizeof(int)) goto desconectar;

                int grupo_idx = pool->grupo_idx_cliente[i];
                pthread_mutex_lock(&mutex_grupos);
                GrupoConsumidor *grupo = &grupos[grupo_idx];
                int turno = grupo->turno % grupo->num_consumidores;
                ConsumidorNodo* actual = grupo->consumidores;
                for (int t = 0; t < turno && actual; ++t) {
                    actual = actual->siguiente;
                }
                // actual ahora apunta al consumidor al que le toca
                int es_turno = (actual->id == pool->id_consumidor_cliente[i]);
                pthread_mutex_unlock(&mutex_grupos);

                // Espera a que haya mensajes disponibles
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                sumar_milisegundos(&ts, 200); // 200 ms

                while (!terminar && sem_timedwait(&sem_mensajes_disponibles, &ts) == -1 && errno == ETIMEDOUT) {
                    clock_gettime(CLOCK_REALTIME, &ts);
                    sumar_milisegundos(&ts, 200);
                }
                if (terminar) break;

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
                    struct timeval tv;
                    tv.tv_sec = 0;
                    tv.tv_usec = 500000; // 500 ms de timeout

                    fd_set fds;
                    FD_ZERO(&fds);
                    FD_SET(sock, &fds);

                    int ready = select(sock + 1, &fds, NULL, NULL, &tv);
                    if (ready > 0) {
                        // El consumidor respondió, procesar normalmente
                        send(sock, &m, sizeof(Mensaje), 0);

                        EventoConsumo e;
                        e.mensaje_id = m.id;
                        e.productor_socket = m.productor_socket;
                        strncpy(e.grupo, grupo->nombre, LONGITUD_NOMBRE_GRUPO);
                        e.consumidor_id = pool->id_consumidor_cliente[i];
                        e.timestamp = time(NULL);
                        agregar_a_log_consumo(e);

                        pthread_mutex_lock(&mutex_grupos);
                        grupo->offset++;
                        grupo->turno = (grupo->turno + 1) % grupo->num_consumidores;
                        pthread_mutex_unlock(&mutex_grupos);

                        pthread_mutex_lock(&mutex_limpieza);
                        pthread_cond_signal(&cond_limpieza);
                        pthread_mutex_unlock(&mutex_limpieza);
                    } else {
                        // Timeout: avanzar turno aunque este consumidor no respondió
                        pthread_mutex_lock(&mutex_grupos);
                        grupo->turno = (grupo->turno + 1) % grupo->num_consumidores;
                        pthread_mutex_unlock(&mutex_grupos);
                    }
                } else {
                    Mensaje vacio = {.id = -1};
                    strcpy(vacio.contenido, "VACIO");
                    vacio.timestamp = 0;
                    send(sock, &vacio, sizeof(Mensaje), 0);
                }

                // Señala que hay un espacio libre en la cola
                sem_post(&sem_espacios_libres);

                continue;
            }

        desconectar:
            printf("[BROKER] Cliente desconectado: socket=%d\n", sock);
            // Si era consumidor, eliminar solo el consumidor correspondiente del grupo
            if (pool->tipo_cliente[i] == 'C') {
                int grupo_idx = pool->grupo_idx_cliente[i];
                int cid = pool->id_consumidor_cliente[i];
                if (grupo_idx >= 0 && cid >= 0) {
                    pthread_mutex_lock(&mutex_grupos);
                    GrupoConsumidor *grupo = &grupos[grupo_idx];
                    // Elimina solo el consumidor correspondiente
                    ConsumidorNodo **ptr = &grupo->consumidores;
                    while (*ptr) {
                        if ((*ptr)->id == cid) {
                            ConsumidorNodo* temp = *ptr;
                            *ptr = (*ptr)->siguiente;
                            free(temp);
                            grupo->num_consumidores--;
                            break;
                        }
                        ptr = &(*ptr)->siguiente;
                    }
                    pthread_mutex_unlock(&mutex_grupos);
                }
            }
            close(sock);
            // Mueve el último cliente al hueco y reduce cantidad_sockets
            int last = pool->cantidad_sockets - 1;
            if (i != last) {
                pool->sockets[i] = pool->sockets[last];
                pool->tipo_cliente[i] = pool->tipo_cliente[last];
                pool->estado_cliente[i] = pool->estado_cliente[last];
                pool->grupo_idx_cliente[i] = pool->grupo_idx_cliente[last];
                pool->id_consumidor_cliente[i] = pool->id_consumidor_cliente[last];
            }
            pool->cantidad_sockets--;
            i--; // para no saltar el nuevo elemento en la posición i
        }
        pthread_mutex_unlock(&pool->mutex);
    }
    printf("Hilo worker %d terminando\n", thread_index);
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

    // Inicializa los semáforos después de configurar la cola
    sem_init(&sem_espacios_libres, 0, LONGITUD_MAXIMA_MENSAJES);
    sem_init(&sem_mensajes_disponibles, 0, 0);

    // Crear hilo de persistencia
    pthread_t hilo_persistencia;
    pthread_create(&hilo_persistencia, NULL, hilo_persistencia_func, NULL);

    // Crear hilo de limpieza
    pthread_t hilo_limpieza;
    pthread_create(&hilo_limpieza, NULL, hilo_limpieza_func, NULL);

    // Crear hilo de log de consumo
    pthread_t hilo_log_consumo;
    pthread_create(&hilo_log_consumo, NULL, hilo_log_consumo_func, NULL);

    // Crear socket del servidor
    // 'servidor' ya está declarado como global
    servidor = socket(AF_INET, SOCK_STREAM, 0); // Usa la global

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
        pool_sockets_array[i].capacidad = 32; // capacidad inicial
        pool_sockets_array[i].sockets = malloc(pool_sockets_array[i].capacidad * sizeof(int));
        pool_sockets_array[i].tipo_cliente = malloc(pool_sockets_array[i].capacidad * sizeof(char));
        pool_sockets_array[i].estado_cliente = malloc(pool_sockets_array[i].capacidad * sizeof(int));
        pool_sockets_array[i].grupo_idx_cliente = malloc(pool_sockets_array[i].capacidad * sizeof(int));
        pool_sockets_array[i].id_consumidor_cliente = malloc(pool_sockets_array[i].capacidad * sizeof(int));
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
        if (pool->cantidad_sockets >= pool->capacidad) {
            int nueva_cap = pool->capacidad * 2;
            pool->sockets = realloc(pool->sockets, nueva_cap * sizeof(int));
            pool->tipo_cliente = realloc(pool->tipo_cliente, nueva_cap * sizeof(char));
            pool->estado_cliente = realloc(pool->estado_cliente, nueva_cap * sizeof(int));
            pool->grupo_idx_cliente = realloc(pool->grupo_idx_cliente, nueva_cap * sizeof(int));
            pool->id_consumidor_cliente = realloc(pool->id_consumidor_cliente, nueva_cap * sizeof(int));
            pool->capacidad = nueva_cap;
        }
        pool->sockets[pool->cantidad_sockets] = socket_cliente;
        pool->tipo_cliente[pool->cantidad_sockets] = 0;
        pool->estado_cliente[pool->cantidad_sockets] = 0;
        pool->grupo_idx_cliente[pool->cantidad_sockets] = -1;
        pool->id_consumidor_cliente[pool->cantidad_sockets] = -1;
        pool->cantidad_sockets++;
        printf("[BROKER] Nueva conexión aceptada: socket=%d asignado al pool/hilo %d\n", socket_cliente, min_idx);
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
    pthread_join(hilo_log_consumo, NULL);

    // Destruye los semáforos
    sem_destroy(&sem_espacios_libres);
    sem_destroy(&sem_mensajes_disponibles);

    // Liberar memoria dinámica
    free(pool_hilos);

    close(servidor);
    munmap(cola, sizeof(ColaMensajes));
    close(shm_fd);
    shm_unlink(NOMBRE_MEMORIA);
    
    // Asegurar que todo lo pendiente se escriba
    flush_persistencia();
    
    // Liberar todos los consumidores de todos los grupos al final
    for (int g = 0; g < num_grupos; ++g) {
        liberar_consumidores(grupos[g].consumidores);
        grupos[g].consumidores = NULL;
        grupos[g].num_consumidores = 0;
    }

    // Liberar la memoria dinámica de cada pool
    for (int i = 0; i < tamano_pool_hilos; ++i) {
        free(pool_sockets_array[i].sockets);
        free(pool_sockets_array[i].tipo_cliente);
        free(pool_sockets_array[i].estado_cliente);
        free(pool_sockets_array[i].grupo_idx_cliente);
        free(pool_sockets_array[i].id_consumidor_cliente);
    }
    free(pool_sockets_array);

    return 0;
    //probar con gcc broker.c -o broker -lpthread
}

void liberar_consumidores(ConsumidorNodo* cabeza) {
    ConsumidorNodo* actual = cabeza;
    while (actual) {
        ConsumidorNodo* siguiente = actual->siguiente;
        free(actual);
        actual = siguiente;
    }
}
