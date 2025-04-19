// broker.c - Broker que maneja múltiples conexiones de productores y consumidores mediante hilos

// =======================
//  INCLUDES Y DEFINES
// =======================
#define _POSIX_C_SOURCE 200809L
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

#define PUERTO 4444
#define MAX_CONEXIONES 10
#define LONGITUD_MAXIMA_MENSAJE 256
#define LONGITUD_MAXIMA_MENSAJES 100
#define NOMBRE_MEMORIA "/memoria_cola_mensajes"
#define MAX_CONSUMIDORES 32
#define MAX_GRUPOS 16
#define MAX_CONSUMIDORES_POR_GRUPO 16
#define LONGITUD_NOMBRE_GRUPO 32
#define TAMANO_POOL_HILOS 8
#define TAMANO_COLA_TAREAS 64

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
    int base_offset; // Offset absoluto del mensaje en 'frente'
} ColaMensajes;

typedef struct {
    Mensaje mensajes[LONGITUD_MAXIMA_MENSAJES];
    int frente;
    int final;
} ColaPersistencia;

typedef struct {
    int activo;
    int offset; // Cuántos mensajes ha leído este consumidor
} OffsetConsumidor;

typedef struct {
    char nombre[LONGITUD_NOMBRE_GRUPO];
    int consumidores[MAX_CONSUMIDORES_POR_GRUPO]; // IDs de consumidores activos
    int num_consumidores;
    int offset; // Offset de grupo (absoluto)
    int turno;  // Índice del consumidor al que le toca leer
} GrupoConsumidor;

// =======================
//  VARIABLES GLOBALES
// =======================
ColaMensajes *cola;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

ColaPersistencia cola_persistencia = {.frente = 0, .final = 0};
pthread_mutex_t mutex_persistencia = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_persistencia = PTHREAD_COND_INITIALIZER;

OffsetConsumidor offsets[MAX_CONSUMIDORES] = {0};
pthread_mutex_t mutex_offsets = PTHREAD_MUTEX_INITIALIZER;

int ids_libres[MAX_CONSUMIDORES];      // Pila de IDs libres
int tope_libres = 0;                   // Tope de la pila

GrupoConsumidor grupos[MAX_GRUPOS];
int num_grupos = 0;
pthread_mutex_t mutex_grupos = PTHREAD_MUTEX_INITIALIZER;

// Pool de hilos
int cola_tareas[TAMANO_COLA_TAREAS];
int frente_tareas = 0, final_tareas = 0;
pthread_mutex_t mutex_tareas = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_tareas = PTHREAD_COND_INITIALIZER;

// Sincronización adicional
pthread_mutex_t mutex_limpieza = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_limpieza = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex_mensajes = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_mensajes = PTHREAD_COND_INITIALIZER;

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
//  FUNCIONES DE COLA DE PERSISTENCIA
// =======================
int insertar_persistencia(ColaPersistencia *cola, Mensaje m) {
    int next = (cola->final + 1) % LONGITUD_MAXIMA_MENSAJES;
    if (next == cola->frente) return -1; // llena
    cola->mensajes[cola->final] = m;
    cola->final = next;
    return 0;
}

int extraer_persistencia(ColaPersistencia *cola, Mensaje *m) {
    if (cola->frente == cola->final) return -1; // vacía
    *m = cola->mensajes[cola->frente];
    cola->frente = (cola->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
    return 0;
}

// =======================
//  FUNCIONES DE GRUPOS Y OFFSETS
// =======================
int obtener_offset_minimo_grupos() {
    int min_offset = INT_MAX;
    pthread_mutex_lock(&mutex_grupos);
    for (int i = 0; i < num_grupos; ++i) {
        if (grupos[i].offset < min_offset) {
            min_offset = grupos[i].offset;
        }
    }
    pthread_mutex_unlock(&mutex_grupos);
    return min_offset;
}

int insertar_mensaje_con_retencion(ColaMensajes *c, Mensaje m) {
    while (esta_llena(c)) {
        int min_offset = obtener_offset_minimo_grupos();
        if (min_offset <= c->base_offset) {
            // No se puede eliminar el mensaje más antiguo, la cola está llena y algún grupo no ha leído
            return -1;
        }
        // Todos los grupos ya leyeron el mensaje más antiguo, avanza el frente
        c->frente = (c->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
        c->base_offset++;
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
//  HILOS AUXILIARES (PERSISTENCIA Y LIMPIEZA)
// =======================
void* hilo_persistencia_func(void* arg) {
    while (1) {
        pthread_mutex_lock(&mutex_persistencia);
        while (cola_persistencia.frente == cola_persistencia.final) {
            pthread_cond_wait(&cond_persistencia, &mutex_persistencia);
        }
        Mensaje m;
        extraer_persistencia(&cola_persistencia, &m);
        pthread_mutex_unlock(&mutex_persistencia);

        FILE *f = fopen("log_mensajes.txt", "a");
        if (f) {
            fprintf(f, "ID: %d | %s | %ld\n", m.id, m.contenido, m.timestamp);
            printf("[PERSISTENCIA] Mensaje guardado: %s\n", m.contenido);
            fclose(f);
        } else {
            printf("[PERSISTENCIA] Error al abrir el archivo de log\n");
        }
    }
    return NULL;
}

void* hilo_limpieza_func(void* arg) {
    while (1) {
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
//  MANEJO DE CLIENTES (PRODUCTOR/CONSUMIDOR)
// =======================
void* manejar_cliente(void *arg) {
    int socket_cliente = *(int*)arg;
    free(arg);

    char tipo;
    if (recv(socket_cliente, &tipo, 1, 0) <= 0) {
        close(socket_cliente);
        return NULL;
    }

    if (tipo == 'P') { // Productor
        while (1) {
            Mensaje m;
            int r = recv(socket_cliente, &m, sizeof(Mensaje), 0);
            if (r <= 0) break; // conexión cerrada o error

            pthread_mutex_lock(&mutex);
            int resultado_insercion = insertar_mensaje_con_retencion(cola, m);
            pthread_mutex_unlock(&mutex);

            send(socket_cliente, (resultado_insercion == 0) ? "OK" : "LLENO", 6, 0);

            // Insertar en la cola de persistencia
            pthread_mutex_lock(&mutex_persistencia);
            int res = insertar_persistencia(&cola_persistencia, m);
            if (res == 0) {
                printf("[BROKER] Mensaje insertado en la cola de persistencia: %s\n", m.contenido);
                pthread_cond_signal(&cond_persistencia);
            } else {
                printf("[BROKER] Cola de persistencia LLENA, mensaje perdido: %s\n", m.contenido);
            }
            pthread_mutex_unlock(&mutex_persistencia);

            pthread_mutex_lock(&mutex_mensajes);
            pthread_cond_broadcast(&cond_mensajes); // Despierta a todos los consumidores
            pthread_mutex_unlock(&mutex_mensajes);
        }
    } else if (tipo == 'C') { // Consumidor
        char nombre_grupo[LONGITUD_NOMBRE_GRUPO];
        if (recv(socket_cliente, nombre_grupo, LONGITUD_NOMBRE_GRUPO, 0) <= 0) {
            close(socket_cliente);
            return NULL;
        }

        // Buscar o crear el grupo
        pthread_mutex_lock(&mutex_grupos);
        int grupo_idx = -1;
        for (int i = 0; i < num_grupos; ++i) {
            if (strcmp(grupos[i].nombre, nombre_grupo) == 0) {
                grupo_idx = i;
                break;
            }
        }
        // Al crear un grupo nuevo, imprime el offset inicial y el base_offset para depuración:
        if (grupo_idx == -1 && num_grupos < MAX_GRUPOS) {
            grupo_idx = num_grupos++;
            strncpy(grupos[grupo_idx].nombre, nombre_grupo, LONGITUD_NOMBRE_GRUPO);
            grupos[grupo_idx].num_consumidores = 0;
            grupos[grupo_idx].offset = cola->base_offset; // Offset absoluto inicial
            grupos[grupo_idx].turno = 0;
        }
        if (grupo_idx == -1) {
            pthread_mutex_unlock(&mutex_grupos);
            send(socket_cliente, "GRUPO_NO_DISPONIBLE", 20, 0);
            close(socket_cliente);
            return NULL;
        }
        pthread_mutex_unlock(&mutex_grupos);

        // Asignar ID único al consumidor
        int id_consumidor = -1;
        pthread_mutex_lock(&mutex_offsets);
        if (tope_libres > 0) {
            id_consumidor = ids_libres[--tope_libres]; // Pop de la pila
            offsets[id_consumidor].activo = 1;
            offsets[id_consumidor].offset = grupos[grupo_idx].offset; // Offset absoluto
        }
        pthread_mutex_unlock(&mutex_offsets);

        if (id_consumidor == -1) {
            // No hay espacio para más consumidores
            close(socket_cliente);
            return NULL;
        }

        // Enviar el id al consumidor
        send(socket_cliente, &id_consumidor, sizeof(int), 0);

        pthread_mutex_lock(&mutex_grupos);
        GrupoConsumidor *grupo = &grupos[grupo_idx];
        if (grupo->num_consumidores < MAX_CONSUMIDORES_POR_GRUPO) {
            grupo->consumidores[grupo->num_consumidores++] = id_consumidor;
        } else {
            pthread_mutex_unlock(&mutex_grupos);
            send(socket_cliente, "GRUPO_LLENO", 12, 0);
            close(socket_cliente);
            return NULL;
        }
        pthread_mutex_unlock(&mutex_grupos);

        while (1) {
            char peticion;
            int r = recv(socket_cliente, &peticion, 1, 0);
            if (r <= 0) break;
            if (peticion != 'R') continue;

            int id_recv;
            r = recv(socket_cliente, &id_recv, sizeof(int), 0);
            if (r <= 0) break;

            int mensaje_enviado = 0;
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 10; // Timeout de 10 segundos

            
            while (!mensaje_enviado) {
                pthread_mutex_lock(&mutex_grupos);
                int es_turno = (grupo->consumidores[grupo->turno] == id_recv);
                pthread_mutex_unlock(&mutex_grupos);

                pthread_mutex_lock(&mutex);
                int cantidad_en_cola = (cola->final - cola->frente + LONGITUD_MAXIMA_MENSAJES) % LONGITUD_MAXIMA_MENSAJES;
                int hay_mensaje = grupo->offset >= cola->base_offset && grupo->offset < cola->base_offset + cantidad_en_cola;

                if (es_turno && hay_mensaje) {
                    int idx = (cola->frente + (grupo->offset - cola->base_offset)) % LONGITUD_MAXIMA_MENSAJES;
                    Mensaje m = cola->mensajes[idx];
                    send(socket_cliente, &m, sizeof(Mensaje), 0);
                    grupo->offset++;

                    pthread_mutex_lock(&mutex_grupos);
                    grupo->turno = (grupo->turno + 1) % grupo->num_consumidores;
                    pthread_mutex_unlock(&mutex_grupos);

                    // Señaliza al hilo de limpieza para que revise la cola
                    pthread_mutex_lock(&mutex_limpieza);
                    pthread_cond_signal(&cond_limpieza);
                    pthread_mutex_unlock(&mutex_limpieza);

                    mensaje_enviado = 1;
                } else {
                    // Espera a que haya mensajes nuevos o timeout
                    pthread_mutex_lock(&mutex_mensajes);
                    pthread_mutex_unlock(&mutex); // Libera el mutex principal antes de esperar
                    int wait_res = pthread_cond_timedwait(&cond_mensajes, &mutex_mensajes, &ts);
                    pthread_mutex_unlock(&mutex_mensajes);
                    if (wait_res == ETIMEDOUT) break;
                }
                if (mensaje_enviado) pthread_mutex_unlock(&mutex);
            }
            if (!mensaje_enviado) {
                // Si después del timeout no hay mensaje, responde "VACIO"
                Mensaje vacio;
                vacio.id = -1;
                strcpy(vacio.contenido, "VACIO");
                vacio.timestamp = 0;
                send(socket_cliente, &vacio, sizeof(Mensaje), 0);
            }
        }

        pthread_mutex_lock(&mutex_offsets);
        offsets[id_consumidor].activo = 0;
        ids_libres[tope_libres++] = id_consumidor; // Push a la pila
        pthread_mutex_unlock(&mutex_offsets);

        pthread_mutex_lock(&mutex_grupos);
        int idx_out = -1;
        for (int i = 0; i < grupo->num_consumidores; ++i) {
            if (grupo->consumidores[i] == id_consumidor) {
                idx_out = i;
                break;
            }
        }
        if (idx_out != -1) {
            for (int i = idx_out; i < grupo->num_consumidores - 1; ++i) {
                grupo->consumidores[i] = grupo->consumidores[i + 1];
            }
            grupo->num_consumidores--;
            if (grupo->turno >= grupo->num_consumidores && grupo->num_consumidores > 0)
                grupo->turno = 0;
            // Si el grupo queda vacío, elimínalo para liberar recursos
            if (grupo->num_consumidores == 0) {
                // Mueve el último grupo a la posición actual y reduce num_grupos
                int grupo_idx = grupo - grupos;
                if (grupo_idx != num_grupos - 1) {
                    grupos[grupo_idx] = grupos[num_grupos - 1];
                }
                num_grupos--;
            }
        }
        pthread_mutex_unlock(&mutex_grupos);
    }

    close(socket_cliente);
    return NULL;
}

// =======================
//  THREAD POOL: WORKER
// =======================
void* trabajador(void* arg) {
    while (1) {
        pthread_mutex_lock(&mutex_tareas);
        while (frente_tareas == final_tareas)
            pthread_cond_wait(&cond_tareas, &mutex_tareas);
        int socket_cliente = cola_tareas[frente_tareas];
        frente_tareas = (frente_tareas + 1) % TAMANO_COLA_TAREAS;
        pthread_mutex_unlock(&mutex_tareas);

        int *pcliente = malloc(sizeof(int));
        if (!pcliente) {
            close(socket_cliente);
            continue;
        }
        *pcliente = socket_cliente;
        manejar_cliente(pcliente); // manejar_cliente libera pcliente y cierra el socket
    }
    return NULL;
}

// =======================
//  MAIN
// =======================
int main() {
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

    pthread_t pool_hilos[TAMANO_POOL_HILOS];
    for (int i = 0; i < TAMANO_POOL_HILOS; ++i)
        pthread_create(&pool_hilos[i], NULL, trabajador, NULL);

    while (1) {
        int socket_cliente = accept(servidor, NULL, NULL);
        if (socket_cliente == -1) {
            perror("accept");
            continue;
        }
        pthread_mutex_lock(&mutex_tareas);
        int siguiente_final = (final_tareas + 1) % TAMANO_COLA_TAREAS;
        if (siguiente_final == frente_tareas) {
            // Cola llena, rechaza conexión
            close(socket_cliente);
        } else {
            cola_tareas[final_tareas] = socket_cliente;
            final_tareas = siguiente_final;
            pthread_cond_signal(&cond_tareas);
        }
        pthread_mutex_unlock(&mutex_tareas);
    }

    close(servidor);
    munmap(cola, sizeof(ColaMensajes));
    close(shm_fd);
    shm_unlink(NOMBRE_MEMORIA);
    return 0;
}
