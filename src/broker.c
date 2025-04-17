// broker.c - Broker que maneja múltiples conexiones de productores y consumidores mediante hilos

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

#define PUERTO 4444
#define MAX_CONEXIONES 10
#define LONGITUD_MAXIMA_MENSAJE 256
#define LONGITUD_MAXIMA_MENSAJES 100
#define NOMBRE_MEMORIA "/memoria_cola_mensajes"
#define MAX_CONSUMIDORES 32

// Estructura de mensaje individual
typedef struct {
    int id;
    char contenido[LONGITUD_MAXIMA_MENSAJE];
    time_t timestamp;
} Mensaje;

// Estructura de la cola circular de mensajes
typedef struct {
    Mensaje mensajes[LONGITUD_MAXIMA_MENSAJES];
    int frente;
    int final;
} ColaMensajes;

// Estructura de la cola de persistencia
typedef struct {
    Mensaje mensajes[LONGITUD_MAXIMA_MENSAJES];
    int frente;
    int final;
} ColaPersistencia;

// Estructura para manejar offsets de consumidores
typedef struct {
    int activo;
    int offset; // Cuántos mensajes ha leído este consumidor
} OffsetConsumidor;

// Variables globales
ColaMensajes *cola;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

ColaPersistencia cola_persistencia = {.frente = 0, .final = 0};
pthread_mutex_t mutex_persistencia = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_persistencia = PTHREAD_COND_INITIALIZER;

OffsetConsumidor offsets[MAX_CONSUMIDORES] = {0};
pthread_mutex_t mutex_offsets = PTHREAD_MUTEX_INITIALIZER;

int ids_libres[MAX_CONSUMIDORES];      // Pila de IDs libres
int tope_libres = 0;                   // Tope de la pila

// Funciones para operar con la cola
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
    return 0;
}

// Inserta un mensaje en la cola de persistencia
int insertar_persistencia(ColaPersistencia *cola, Mensaje m) {
    int next = (cola->final + 1) % LONGITUD_MAXIMA_MENSAJES;
    if (next == cola->frente) return -1; // llena
    cola->mensajes[cola->final] = m;
    cola->final = next;
    return 0;
}

// Extrae un mensaje de la cola de persistencia
int extraer_persistencia(ColaPersistencia *cola, Mensaje *m) {
    if (cola->frente == cola->final) return -1; // vacía
    *m = cola->mensajes[cola->frente];
    cola->frente = (cola->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
    return 0;
}

// Hilo de persistencia
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

// Función que maneja cada conexión de cliente en un hilo
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
            int resultado_insercion = insertar_mensaje(cola, m);
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
        }
    } else if (tipo == 'C') { // Consumidor
        // Asignar ID único al consumidor
        int id_consumidor = -1;
        pthread_mutex_lock(&mutex_offsets);
        if (tope_libres > 0) {
            id_consumidor = ids_libres[--tope_libres]; // Pop de la pila
            offsets[id_consumidor].activo = 1;
            offsets[id_consumidor].offset = 0;
        }
        pthread_mutex_unlock(&mutex_offsets);

        if (id_consumidor == -1) {
            // No hay espacio para más consumidores
            close(socket_cliente);
            return NULL;
        }

        // Enviar el id al consumidor
        send(socket_cliente, &id_consumidor, sizeof(int), 0);

        while (1) {
            char peticion;
            int r = recv(socket_cliente, &peticion, 1, 0);
            if (r <= 0) break; // conexión cerrada o error
            if (peticion != 'R') continue; // ignorar si no es petición válida

            // El consumidor debe enviar su id en cada petición
            int id_recv;
            r = recv(socket_cliente, &id_recv, sizeof(int), 0);
            if (r <= 0) break;

            pthread_mutex_lock(&mutex);
            int offset = offsets[id_recv].offset;
            int mensajes_disponibles = (cola->final - cola->frente + LONGITUD_MAXIMA_MENSAJES) % LONGITUD_MAXIMA_MENSAJES;
            if (offset < mensajes_disponibles) {
                int idx = (cola->frente + offset) % LONGITUD_MAXIMA_MENSAJES;
                Mensaje m = cola->mensajes[idx];
                send(socket_cliente, &m, sizeof(Mensaje), 0);
                offsets[id_recv].offset++;
            } else {
                send(socket_cliente, "VACIO", 6, 0);
            }
            pthread_mutex_unlock(&mutex);
        }

        pthread_mutex_lock(&mutex_offsets);
        offsets[id_consumidor].activo = 0;
        ids_libres[tope_libres++] = id_consumidor; // Push a la pila
        pthread_mutex_unlock(&mutex_offsets);
    }

    close(socket_cliente);
    return NULL;
}

int main() {
    // Configurar memoria compartida
    int shm_fd = shm_open(NOMBRE_MEMORIA, O_CREAT | O_RDWR, 0666);
    ftruncate(shm_fd, sizeof(ColaMensajes));
    cola = mmap(NULL, sizeof(ColaMensajes), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    cola->frente = cola->final = 0;

    // Inicializar la pila de IDs libres
    for (int i = 0; i < MAX_CONSUMIDORES; ++i) {
        ids_libres[i] = MAX_CONSUMIDORES - 1 - i; // IDs del mayor al menor para pop eficiente
    }
    tope_libres = MAX_CONSUMIDORES;

    // Crear hilo de persistencia
    pthread_t hilo_persistencia;
    pthread_create(&hilo_persistencia, NULL, hilo_persistencia_func, NULL);

    // Crear socket del servidor
    int servidor = socket(AF_INET, SOCK_STREAM, 0); //Aqui se define el socket del servidor
   //AF_INET es para IPv4, SOCK_STREAM es para TCP y 0 es para el protocolo por defecto
    struct sockaddr_in addr = {0};
   //en esta linea se inicializa la estructura sockaddr_in a cero 
   //en estas estructuras se define la direccion del socket, las direcciones IP y el puerto
    addr.sin_family = AF_INET;// AF_INET es para IPv4
    addr.sin_addr.s_addr = INADDR_ANY;// INADDR_ANY permite que el socket escuche en todas las interfaces de red disponibles
    addr.sin_port = htons(PUERTO);// htons convierte el número de puerto a la representación de red (big-endian)

    bind(servidor, (struct sockaddr*)&addr, sizeof(addr)); // Asocia el socket a la dirección y puerto especificados
    listen(servidor, MAX_CONEXIONES);// Escucha conexiones entrantes, permitiendo hasta MAX_CONEXIONES en la cola de espera
    printf("[BROKER] Escuchando en el puerto %d...\n", PUERTO); //Imprime el mensaje de que el broker está escuchando en el puerto especificado

    while (1) {
        int *cliente = malloc(sizeof(int));//Esto sirve para almacenar el socket del cliente es int ya que el socket es un entero
        //y se usa malloc para asignar memoria dinámica para el socket del cliente debido a que se va a crear un hilo para manejar la conexión
        *cliente = accept(servidor, NULL, NULL);// el accept() acepta una conexión entrante y devuelve un nuevo socket para la comunicación con el cliente
        pthread_t hilo;
        pthread_create(&hilo, NULL, manejar_cliente, cliente);// crea un nuevo hilo para manejar la conexión del cliente
        pthread_detach(hilo);// separa el hilo para que se limpie automáticamente cuando termine
        //pthread_detach(hilo) permite que el hilo se ejecute en segundo plano y se limpie automáticamente cuando termine
        //sin necesidad de esperar a que termine el hilo
    }

    close(servidor);
    munmap(cola, sizeof(ColaMensajes));
    close(shm_fd);
    shm_unlink(NOMBRE_MEMORIA);
    return 0;
}
