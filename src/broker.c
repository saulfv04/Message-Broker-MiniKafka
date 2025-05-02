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
#include <signal.h>
#include <sys/select.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <sys/resource.h>
#include <poll.h>

#define _XOPEN_SOURCE 700
#define PUERTO 4444
#define LONGITUD_MAXIMA_MENSAJE 256
#define LONGITUD_MAXIMA_MENSAJES 1024
#define NOMBRE_MEMORIA "/memoria_cola_mensajes"

#define MAX_GRUPOS 50
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

} ColaMensajes;

typedef struct ConsumidorNodo {
    int id;
    struct ConsumidorNodo* siguiente;
} ConsumidorNodo;

// --- PROBLEMA DE DUPLICACIÓN ---
// Si varios consumidores del mismo grupo consumen a la vez, pueden leer el mismo mensaje antes de que el offset avance.
// SOLUCIÓN: Añadimos un mutex por grupo para proteger la lógica de consumo y evitar duplicados.
typedef struct {
    char nombre[LONGITUD_NOMBRE_GRUPO];
    ConsumidorNodo* consumidores;
    int num_consumidores;
    int offset; // Offset de grupo (absoluto)
    pthread_mutex_t mutex; // Mutex específico para este grupo
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
atomic_int stop_accept = 0;
volatile sig_atomic_t terminar = 0;

// Declarar 'servidor' como variable global para acceso en el handler
int servidor = -1;

// Manejador de señales
void handler(int sig) {
    printf("Recibida señal %d, deteniendo accept()\n", sig);
    atomic_store(&stop_accept, 1);
    if (servidor != -1) close(servidor);

    pthread_mutex_lock(&mutex_buffer);
    pthread_cond_broadcast(&cond_buffer);
    pthread_mutex_unlock(&mutex_buffer);

    pthread_mutex_lock(&mutex_limpieza);
    pthread_cond_broadcast(&cond_limpieza);
    pthread_mutex_unlock(&mutex_limpieza);

    pthread_mutex_lock(&mutex_log);
    pthread_cond_broadcast(&cond_log);
    pthread_mutex_unlock(&mutex_log);
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

pthread_cond_t cond_productores = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex_productores = PTHREAD_MUTEX_INITIALIZER;

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
    if (esta_llena(c)) {
        int min_offset = obtener_offset_minimo_grupos();
        if (min_offset > c->base_offset) {
            c->frente = (c->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
            c->base_offset++;
        } else {
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
    printf("[DEBUG] Mensaje aceptado para persistencia: ID=%d\n", m.id);
    buffer_escritura[idx_escritura++] = m;
    if (idx_escritura == BUFFER_PERSISTENCIA ||
        ((terminar || atomic_load(&stop_accept)) && idx_escritura > 0)) {
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
        fprintf(stderr,
            "[PERSI] FLUSH forzando swap buffer_%c, idx_escritura=%d\n",
            (buffer_escritura == buffer_a ? 'A' : 'B'),
            idx_escritura);
        Mensaje *tmp = buffer_escritura;
        buffer_escritura = buffer_lectura;
        buffer_lectura = tmp;
        idx_lectura = idx_escritura;
        idx_escritura = 0;
        fprintf(stderr,
            "[PERSI] FLUSH completado: buffer_lectura=%c, idx_lectura=%d\n",
            (buffer_lectura == buffer_a ? 'A' : 'B'),
            idx_lectura);
        pthread_cond_signal(&cond_buffer);
    }
    pthread_mutex_unlock(&mutex_buffer);
}

void agregar_a_log_consumo(EventoConsumo e) {
    pthread_mutex_lock(&mutex_log);
    buffer_log_escritura[idx_log_escritura++] = e;
    fprintf(stderr, "[LOG] Evento agregado: mensaje_id=%d, consumidor_id=%d, idx_log_escritura=%d\n",
            e.mensaje_id, e.consumidor_id, idx_log_escritura);
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

void flush_log_consumo() {
    pthread_mutex_lock(&mutex_log);
    if (idx_log_escritura > 0) {
        EventoConsumo *tmp = buffer_log_escritura;
        buffer_log_escritura = buffer_log_lectura;
        buffer_log_lectura = tmp;
        idx_log_lectura = idx_log_escritura;
        idx_log_escritura = 0;
        pthread_cond_signal(&cond_log);
    }
    pthread_mutex_unlock(&mutex_log);
}

void* hilo_flush_periodico(void* arg) {
    while (!terminar) {
        sleep(5); // cada 5 segundos
        flush_persistencia();
        flush_log_consumo();
    }
    return NULL;
}

// =======================
//  HILOS AUXILIARES (PERSISTENCIA Y LIMPIEZA)
// =======================
void* hilo_persistencia_func(void* arg) {
    FILE *f = fopen("persistencia_mensajes.txt", "a");
    if (!f) return NULL;
    while (1) {
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
        fflush(f);

        // Si terminar está activo y no quedan pendientes, salimos
        pthread_mutex_lock(&mutex_buffer);
        int quedan_pendientes = (idx_lectura > 0 || idx_escritura > 0);
        int fin = terminar && !quedan_pendientes;
        pthread_mutex_unlock(&mutex_buffer);
        if (fin) break;
    }

    // --- Flush final antes de salir ---
    pthread_mutex_lock(&mutex_buffer);
    int cantidad = idx_lectura;
    idx_lectura = 0;
    pthread_mutex_unlock(&mutex_buffer);
    for (int i = 0; i < cantidad; ++i) {
        Mensaje *m = &buffer_lectura[i];
        fprintf(f, "ID: %d | %s | %ld\n", m->id, m->contenido, m->timestamp);
    }
    pthread_mutex_lock(&mutex_buffer);
    cantidad = idx_escritura;
    idx_escritura = 0;
    pthread_mutex_unlock(&mutex_buffer);
    for (int i = 0; i < cantidad; ++i) {
        Mensaje *m = &buffer_escritura[i];
        fprintf(f, "ID: %d | %s | %ld\n", m->id, m->contenido, m->timestamp);
    }
    fflush(f);
    fclose(f);
    return NULL;
}

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

    struct pollfd *pfds = NULL;
    int pfds_cap = 0;

    while (!terminar) {
        pthread_mutex_lock(&pool->mutex);
        int n = pool->cantidad_sockets;
        // Asegura capacidad del array pollfd
        if (n > pfds_cap) {
            pfds_cap = n * 2;
            pfds = realloc(pfds, pfds_cap * sizeof(struct pollfd));
        }
        for (int i = 0; i < n; ++i) {
            pfds[i].fd = pool->sockets[i];
            pfds[i].events = POLLIN;
        }
        pthread_mutex_unlock(&pool->mutex);

        int ready = poll(pfds, n, 10); // 10 ms timeout
        if (ready < 0) {
            if (errno == EINTR) {
                if (atomic_load(&stop_accept)) break;
                else continue;
            }
            continue;
        }
        if (ready == 0) continue; // Timeout

        pthread_mutex_lock(&pool->mutex);
        for (int i = 0; i < n; ++i) {
            if (!(pfds[i].revents & POLLIN)) continue;
            int sock = pool->sockets[i];

            // Handshake inicial
            if (pool->estado_cliente[i] == 0) {
                char tipo;
                int r = recv(sock, &tipo, 1, MSG_DONTWAIT);
                if (r <= 0) {
                    printf("[DEBUG] Socket %d desconectado antes de handshake (recv=%d, errno=%d)\n", sock, r, errno);
                    goto desconectar;
                }
                pool->tipo_cliente[i] = tipo;
                pool->estado_cliente[i] = 1;
                if (tipo == 'C') {
                    // Asignar al grupo con menos consumidores
                    int grupo_idx = 0;
                    pthread_mutex_lock(&mutex_grupos);
                    for (int g = 1; g < num_grupos; ++g) {
                        if (grupos[g].num_consumidores < grupos[grupo_idx].num_consumidores)
                            grupo_idx = g;
                    }
                    int cid = next_cid++;
                    ConsumidorNodo* nuevo = malloc(sizeof(ConsumidorNodo));
                    nuevo->id = cid;
                    nuevo->siguiente = grupos[grupo_idx].consumidores;
                    grupos[grupo_idx].consumidores = nuevo;
                    grupos[grupo_idx].num_consumidores++;

                    pool->grupo_idx_cliente[i] = grupo_idx;
                    pool->id_consumidor_cliente[i] = cid;
                    send(sock, &cid, sizeof(int), 0);

                    // DEBUG: Mostrar a qué grupo se asignó el consumidor
                    printf("[DEBUG] Consumidor %d asignado al grupo '%s' (grupo_idx=%d)\n", cid, grupos[grupo_idx].nombre, grupo_idx);

                    pthread_mutex_unlock(&mutex_grupos);
                    continue;
                }
                continue;
            }

            // Productor
            if (pool->tipo_cliente[i] == 'P') {
                Mensaje m;
                int r = recv(sock, &m, sizeof(Mensaje), MSG_DONTWAIT);
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

                while (1) {
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    sumar_milisegundos(&ts, 200);

                    if (atomic_load(&stop_accept)) break;
                    if (sem_timedwait(&sem_espacios_libres, &ts) == -1) {
                        if (errno == ETIMEDOUT) continue;
                        else break;
                    }

                    pthread_mutex_lock(&mutex);
                    int resultado_insercion = insertar_mensaje_con_retencion(cola, m);
                    if (resultado_insercion == 0) {
                        pthread_mutex_unlock(&mutex);
                        sem_post(&sem_mensajes_disponibles);
                        send(sock, "OK", 3, MSG_DONTWAIT);
                        break;
                    } else {
                        // No se pudo insertar por retención, liberar espacio y esperar señal
                        sem_post(&sem_espacios_libres);
                        // Esperar a que un consumidor libere espacio
                        pthread_mutex_lock(&mutex_productores);
                        pthread_mutex_unlock(&mutex);
                        pthread_cond_wait(&cond_productores, &mutex_productores);
                        pthread_mutex_unlock(&mutex_productores);
                        continue;
                    }
                }

                // Persistencia (doble buffer)
                // Antes de agregar a persistencia
                printf("[DEBUG] Worker va a persistir mensaje ID=%d\n", m.id);
                agregar_a_persistencia(m);
                printf("[DEBUG] Worker llamó a agregar_a_persistencia para ID=%d\n", m.id);

                pthread_mutex_lock(&mutex_mensajes);
                pthread_cond_broadcast(&cond_mensajes);
                pthread_mutex_unlock(&mutex_mensajes);
                continue;
            }

            // Consumidor
            if (pool->tipo_cliente[i] == 'C') {
                // Espera petición del consumidor
                char peticion;
                int r1 = recv(sock, &peticion, 1, MSG_DONTWAIT);
                if (r1 == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    // No hay datos, sigue esperando
                    continue;
                }
                if (r1 == 0) {
                    printf("[DEBUG] Socket %d: consumidor cerró la conexión (recv=0)\n", sock);
                    goto desconectar;
                }
                if (r1 < 0) {
                    printf("[DEBUG] Socket %d: error en recv peticion (errno=%d: %s)\n", sock, errno, strerror(errno));
                    goto desconectar;
                }

                while (peticion != 'R') {
                    r1 = recv(sock, &peticion, 1, MSG_DONTWAIT);
                    if (r1 == 0) {
                        printf("[DEBUG] Socket %d: consumidor cerró la conexión en espera de 'R' (recv=0)\n", sock);
                        goto desconectar;
                    }
                    if (r1 < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            // No hay datos aún, espera al siguiente ciclo de poll
                            break;
                        }
                        printf("[DEBUG] Socket %d: error en recv esperando 'R' (errno=%d: %s)\n", sock, errno, strerror(errno));
                        goto desconectar;
                    }
                }
                if (peticion != 'R') {
                    // No se recibió el 'R' completo, espera al siguiente ciclo
                    continue;
                }


                int id_recv = 0;
                int total = 0;
                while (total < sizeof(int)) {
                    int r2 = recv(sock, ((char*)&id_recv) + total, sizeof(int) - total, MSG_DONTWAIT);
                    if (r2 == 0) {
                        printf("[DEBUG] Socket %d: consumidor cerró la conexión al enviar id (recv=0)\n", sock);
                        goto desconectar;
                    }
                    if (r2 < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            // No hay datos aún, espera al siguiente ciclo de poll
                            break;
                        }
                        printf("[DEBUG] Socket %d: error en recv id consumidor (errno=%d: %s)\n", sock, errno, strerror(errno));
                        goto desconectar;
                    }
                    total += r2;
                }
                if (total < sizeof(int)) {
                    // No se recibió el ID completo, espera al siguiente ciclo
                    continue;
                }

                int grupo_idx = pool->grupo_idx_cliente[i];
                GrupoConsumidor *grupo = &grupos[grupo_idx];

                // --- PROTECCIÓN CONTRA DUPLICACIÓN ---
                // Solo un consumidor del grupo puede consumir y avanzar el offset a la vez
                pthread_mutex_lock(&grupo->mutex);

                pthread_mutex_lock(&mutex);
                int cantidad_en_cola = (cola->final - cola->frente + LONGITUD_MAXIMA_MENSAJES) % LONGITUD_MAXIMA_MENSAJES;
                int hay_mensaje = grupo->offset >= cola->base_offset && grupo->offset < cola->base_offset + cantidad_en_cola;
                int idxmsg = -1;
                Mensaje m;
                if (hay_mensaje) {
                    idxmsg = (cola->frente + (grupo->offset - cola->base_offset)) % LONGITUD_MAXIMA_MENSAJES;
                    m = cola->mensajes[idxmsg];
                }
                pthread_mutex_unlock(&mutex);

                if (hay_mensaje) {
                    send(sock, &m, sizeof(Mensaje), MSG_DONTWAIT);

                    EventoConsumo e;
                    e.mensaje_id = m.id;
                    e.productor_socket = m.productor_socket;
                    strncpy(e.grupo, grupo->nombre, LONGITUD_NOMBRE_GRUPO);
                    e.consumidor_id = pool->id_consumidor_cliente[i];
                    e.timestamp = time(NULL);
                    agregar_a_log_consumo(e);

                    grupo->offset++; // Avanza el offset del grupo de forma segura

                    pthread_mutex_lock(&mutex_limpieza);
                    pthread_cond_signal(&cond_limpieza);
                    pthread_mutex_unlock(&mutex_limpieza);

                    sem_post(&sem_espacios_libres);
                    pthread_mutex_lock(&mutex_productores);
                    pthread_cond_signal(&cond_productores);
                    pthread_mutex_unlock(&mutex_productores);
                } else {
                    Mensaje vacio = {.id = -1};
                    strcpy(vacio.contenido, "VACIO");
                    vacio.timestamp = 0;
                    send(sock, &vacio, sizeof(Mensaje), MSG_DONTWAIT);
                }
                pthread_mutex_unlock(&grupo->mutex);

                continue;
            }

        desconectar:
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
            printf("[DEBUG] Socket %d desconectado (tipo=%c)\n", sock, pool->tipo_cliente[i]);
            close(sock);
            // Mueve el último cliente al hueco y reduce cantidad_sockets
            int last = pool->cantidad_sockets - 1;
            if (i != last) {
                pool->sockets[i] = pool->sockets[last];
                pool->tipo_cliente[i] = pool->tipo_cliente[last];
                pool->estado_cliente[i] = pool->estado_cliente[last];
                pool->grupo_idx_cliente[i] = pool->grupo_idx_cliente[last];
                pool->id_consumidor_cliente[i] = pool->id_consumidor_cliente[last];
                pfds[i] = pfds[last];
            }
            pool->cantidad_sockets--;
            n--;
            i--;
        }
        pthread_mutex_unlock(&pool->mutex);
    }
    free(pfds);
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

    // Crear hilo de flush periódico
    pthread_t hilo_flush;
    pthread_create(&hilo_flush, NULL, hilo_flush_periodico, NULL);

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

    if (listen(servidor, SOMAXCONN) == -1) {

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

    num_grupos = 3;
    for (int g = 0; g < num_grupos; ++g) {
        snprintf(grupos[g].nombre, LONGITUD_NOMBRE_GRUPO, "grupo_%02d", g + 1);
        grupos[g].num_consumidores = 0;
        grupos[g].consumidores = NULL;
        grupos[g].offset = 0;
        pthread_mutex_init(&grupos[g].mutex, NULL); // Inicializa el mutex por grupo
    }

    struct rlimit lim;
    getrlimit(RLIMIT_NOFILE, &lim);
    printf("[BROKER] RLIMIT_NOFILE: soft=%ld hard=%ld\n", lim.rlim_cur, lim.rlim_max);

    while (!atomic_load(&stop_accept)) {
        int socket_cliente = accept(servidor, NULL, NULL);
        if (atomic_load(&stop_accept)) break;
        if (socket_cliente < 0) {
            perror("accept");
            continue;
        }
        printf("[DEBUG] Socket aceptado: %d\n", socket_cliente);
        // Quitar mensajes de nueva conexión y pool/hilo
        // printf("[BROKER] Nueva conexión aceptada, socket: %d\n", socket_cliente);
        // printf("[BROKER] Nueva conexión aceptada: socket=%d asignado al pool/hilo %d\n", socket_cliente, min_idx);
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
            printf("[DEBUG] Pool %d: realloc a capacidad %d\n", min_idx, nueva_cap);
        }
        pool->sockets[pool->cantidad_sockets] = socket_cliente;
        pool->tipo_cliente[pool->cantidad_sockets] = 0;
        pool->estado_cliente[pool->cantidad_sockets] = 0;
        pool->grupo_idx_cliente[pool->cantidad_sockets] = -1;
        pool->id_consumidor_cliente[pool->cantidad_sockets] = -1;
        pool->cantidad_sockets++;
        pthread_mutex_unlock(&pool->mutex);
    }

    // tras salir del bucle de accept()

    // 1) Forzar swap final del buffer de persistencia bajo mutex
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

    // 1b) Forzar swap final del buffer de log de consumo bajo mutex
    pthread_mutex_lock(&mutex_log);
    if (idx_log_escritura > 0) {
        EventoConsumo *tmp = buffer_log_escritura;
        buffer_log_escritura = buffer_log_lectura;
        buffer_log_lectura = tmp;
        idx_log_lectura = idx_log_escritura;
        idx_log_escritura = 0;
        pthread_cond_signal(&cond_log);
    }
    pthread_mutex_unlock(&mutex_log);

    // 2) Indicar a todos los hilos que terminen
    terminar = 1;

    // 3) Despertar a los auxiliares bloqueados
    pthread_cond_broadcast(&cond_buffer);
    pthread_cond_broadcast(&cond_limpieza);
    pthread_cond_broadcast(&cond_log);

    // 4) Cierra todos los sockets de clientes para desbloquear cualquier recv() bloqueante
    for (int i = 0; i < tamano_pool_hilos; ++i) {
        PoolSockets *pool = &pool_sockets_array[i];
        pthread_mutex_lock(&pool->mutex);
        for (int j = 0; j < pool->cantidad_sockets; ++j) {
            close(pool->sockets[j]);
        }
        pthread_mutex_unlock(&pool->mutex);
    }

    // 5) Esperar a que terminen workers y auxiliares
    for (int i = 0; i < tamano_pool_hilos; ++i) {
        pthread_join(pool_hilos[i], NULL);
    }
    pthread_join(hilo_persistencia, NULL);
    pthread_join(hilo_limpieza,   NULL);
    pthread_join(hilo_log_consumo, NULL);
    pthread_join(hilo_flush, NULL);

    // 6) Cleanup final
    sem_destroy(&sem_espacios_libres);
    sem_destroy(&sem_mensajes_disponibles);
    close(servidor);
    munmap(cola, sizeof(ColaMensajes));
    close(shm_fd);
    shm_unlink(NOMBRE_MEMORIA);
    free(pool_hilos);
    for (int g = 0; g < num_grupos; ++g){
        liberar_consumidores(grupos[g].consumidores);
        pthread_mutex_destroy(&grupos[g].mutex);
    }
    
    // Reporte de sockets huérfanos (sin handshake)
    int huerfanos = 0, consumidores_huerfanos = 0;
    for (int i = 0; i < tamano_pool_hilos; ++i) {
        PoolSockets *pool = &pool_sockets_array[i];
        pthread_mutex_lock(&pool->mutex);
        for (int j = 0; j < pool->cantidad_sockets; ++j) {
            if (pool->estado_cliente[j] == 0) {
                huerfanos++;
                if (pool->tipo_cliente[j] == 'C')
                    consumidores_huerfanos++;
            }
        }
        pthread_mutex_unlock(&pool->mutex);
    }
    printf("[INFO] Sockets huérfanos (sin handshake): %d (de ellos consumidores: %d)\n", huerfanos, consumidores_huerfanos);

    // Ahora sí, libera la memoria
    for (int i = 0; i < tamano_pool_hilos; ++i) {
        free(pool_sockets_array[i].sockets);
        free(pool_sockets_array[i].tipo_cliente);
        free(pool_sockets_array[i].estado_cliente);
        free(pool_sockets_array[i].grupo_idx_cliente);
        free(pool_sockets_array[i].id_consumidor_cliente);
    }
    free(pool_sockets_array);

    pthread_mutex_destroy(&mutex);
    pthread_mutex_destroy(&mutex_grupos);
    pthread_mutex_destroy(&mutex_limpieza);
    pthread_mutex_destroy(&mutex_mensajes);
    pthread_mutex_destroy(&mutex_buffer);
    pthread_mutex_destroy(&mutex_log);
    pthread_mutex_destroy(&mutex_id);

    pthread_cond_destroy(&cond_limpieza);
    pthread_cond_destroy(&cond_mensajes);
    pthread_cond_destroy(&cond_buffer);
    pthread_cond_destroy(&cond_log);

    return 0;
}

void liberar_consumidores(ConsumidorNodo* cabeza) {
    ConsumidorNodo* actual = cabeza;
    while (actual) {
        ConsumidorNodo* siguiente = actual->siguiente;
        free(actual);
        actual = siguiente;
    }
}
