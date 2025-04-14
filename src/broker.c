#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/stat.h>

#define LONGITUD_MAXIMA_MENSAJE 256
#define LONGITUD_MAXIMA_MENSAJES 10
#define NOMBRE_MEMORIA "/memoria_cola_mensajes"

typedef struct {
    int id;
    char contenido[LONGITUD_MAXIMA_MENSAJE];
    time_t timestamp;
} Mensaje;

typedef struct {
    Mensaje mensajes[LONGITUD_MAXIMA_MENSAJES];
    int frente;
    int final;
} ColaMensajes;

void inicializar_cola(ColaMensajes *cola) {
    cola->frente = 0;
    cola->final = 0;
}

int esta_llena(ColaMensajes *cola) {
    return (cola->final + 1) % LONGITUD_MAXIMA_MENSAJES == cola->frente;
}

int esta_vacia(ColaMensajes *cola) {
    return cola->frente == cola->final;
}

int insertar_mensaje(ColaMensajes *cola, Mensaje nuevo) {
    if (esta_llena(cola)) {
        printf("Cola llena. No se puede insertar el mensaje.\n");
        return -1;
    }
    cola->mensajes[cola->final] = nuevo;
    cola->final = (cola->final + 1) % LONGITUD_MAXIMA_MENSAJES;
    return 0;
}

int consumir_mensaje(ColaMensajes *cola, Mensaje *salida) {
    if (esta_vacia(cola)) {
        printf("Cola vacía. No hay mensajes para consumir.\n");
        return -1;
    }
    *salida = cola->mensajes[cola->frente];
    cola->frente = (cola->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
    return 0;
}

void imprimir_mensaje(Mensaje *m) {
    printf(" Mensaje recibido:\n");
    printf("  ID: %d\n", m->id);
    printf("  Contenido: %s\n", m->contenido);
    printf("  Timestamp: %s\n", ctime(&(m->timestamp)));
}

int main() {
    int shm_fd;
    ColaMensajes *cola;

    // Crear y abrir el objeto de memoria compartida
    shm_fd = shm_open(NOMBRE_MEMORIA, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("Error al crear la memoria compartida");
        exit(EXIT_FAILURE);
    }

    // Ajustar el tamaño del objeto de memoria compartida
    if (ftruncate(shm_fd, sizeof(ColaMensajes)) == -1) {
        perror("Error en ftruncate");
        exit(EXIT_FAILURE);
    }

    // Mapear la memoria compartida al espacio de direcciones
    cola = mmap(NULL, sizeof(ColaMensajes), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (cola == MAP_FAILED) {
        perror("Error en mmap");
        exit(EXIT_FAILURE);
    }

    // Inicializar la cola (esto solo se debería hacer una vez, por un proceso inicializador)
    inicializar_cola(cola);

    // Insertar 11 mensajes
    for (int i = 1; i <= 11; i++) {
        Mensaje m;
        m.id = i;
        snprintf(m.contenido, LONGITUD_MAXIMA_MENSAJE, "Este es el mensaje número %d", i);
        m.timestamp = time(NULL);
        insertar_mensaje(cola, m);
    }

    // Consumir mensajes
    for (int i = 0; i < 11; i++) {
        Mensaje recibido;
        if (consumir_mensaje(cola, &recibido) == 0) {
            imprimir_mensaje(&recibido);
        }
    }

    // Desmapear y cerrar memoria compartida
    munmap(cola, sizeof(ColaMensajes));
    close(shm_fd);
    shm_unlink(NOMBRE_MEMORIA);  // Eliminar del sistema

    return 0;
// Compilar con: gcc -o broker src/broker.c -lrt

}
