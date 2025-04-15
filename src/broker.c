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
#define LONGITUD_MAXIMA_MENSAJES 10
#define NOMBRE_MEMORIA "/memoria_cola_mensajes"

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

// Variables globales
ColaMensajes *cola;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

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

// Función que maneja cada conexión de cliente en un hilo
void* manejar_cliente(void *arg) {
    int socket_cliente = *(int*)arg; //socket del cliente
    free(arg);// libera la memoria asignada para el socket del cliente

    char tipo;// tipo de cliente (Productor o Consumidor)
    // Recibir el tipo de cliente (P o C) del socket
    if (recv(socket_cliente, &tipo, 1, 0) <= 0) {// recibe el tipo de cliente mediante el recv y lo almacena en la variable tipo

        close(socket_cliente);
        return NULL;
    }

    if (tipo == 'P') { // Productor
        Mensaje m;
        if (recv(socket_cliente, &m, sizeof(Mensaje), 0) > 0) {
            pthread_mutex_lock(&mutex);
            int resultado_insercion = insertar_mensaje(cola, m);// inserta el mensaje en la cola
            //si este fue exitoso, se desbloquea el mutex y se envía un mensaje de confirmación al productor
            pthread_mutex_unlock(&mutex);
            send(socket_cliente, (resultado_insercion == 0) ? "OK" : "LLENO", 6, 0);// envía un mensaje de confirmación al productor
        }

    } else if (tipo == 'C') { // Consumidor
        pthread_mutex_lock(&mutex);
        Mensaje m;
        int resultado_consumo = consumir_mensaje(cola, &m);
        pthread_mutex_unlock(&mutex);
        if (resultado_consumo == 0) {
            send(socket_cliente, &m, sizeof(Mensaje), 0);
        } else {
            send(socket_cliente, "VACIO", 6, 0);
        }
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
