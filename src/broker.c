
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>

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

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

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
        printf("Cola vacÃ­a. No hay mensajes para consumir.\n");
        return -1;
    }
    *salida = cola->mensajes[cola->frente];
    cola->frente = (cola->frente + 1) % LONGITUD_MAXIMA_MENSAJES;
    return 0;
}

void imprimir_mensaje(Mensaje *m) {
    printf("Mensaje recibido:\n");
    printf("  ID: %d\n", m->id);
    printf("  Contenido: %s\n", m->contenido);
    printf("  Timestamp: %s", ctime(&(m->timestamp)));
}

void log_mensaje(Mensaje *m) {
    FILE *archivo = fopen("log_mensajes.txt", "a");
    if (archivo == NULL) {
        perror("No se pudo abrir el archivo de log");
        return;
    }
    fprintf(archivo, "ID: %d | Contenido: %s | Timestamp: %s",
            m->id, m->contenido, ctime(&(m->timestamp)));
    fclose(archivo);
}

ColaMensajes *cola;

void* productor_hilo(void *arg) {
    int id_base = *(int*)arg;
    for (int i = 0; i < 5; i++) {
        Mensaje m;
        m.id = id_base * 100 + i;
        snprintf(m.contenido, LONGITUD_MAXIMA_MENSAJE, "Mensaje desde productor %d - #%d", id_base, i);
        m.timestamp = time(NULL);

        pthread_mutex_lock(&mutex);
        if (insertar_mensaje(cola, m) == 0) {
            log_mensaje(&m); // registrar en log solo si se insertó
        }
        pthread_mutex_unlock(&mutex);

        sleep(1);
    }
    return NULL;
}
void* consumidor_hilo(void *arg) {
    (void)arg; // sin usar

    for (int i = 0; i < 5; i++) {
        Mensaje recibido;

        pthread_mutex_lock(&mutex);
        if (consumir_mensaje(cola, &recibido) == 0) {
            imprimir_mensaje(&recibido);
        }
        pthread_mutex_unlock(&mutex);

        sleep(2);
    }
    return NULL;
}

int main() {
    int shm_fd;
    shm_fd = shm_open(NOMBRE_MEMORIA, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("Error al crear la memoria compartida");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(shm_fd, sizeof(ColaMensajes)) == -1) {
        perror("Error en ftruncate");
        exit(EXIT_FAILURE);
    }

    cola = mmap(NULL, sizeof(ColaMensajes), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (cola == MAP_FAILED) {
        perror("Error en mmap");
        exit(EXIT_FAILURE);
    }

    inicializar_cola(cola);

    pthread_t productores[3];
    pthread_t consumidores[3];
    int ids[3] = {1, 2, 3};

    for (int i = 0; i < 3; i++) {
        pthread_create(&productores[i], NULL, productor_hilo, &ids[i]);
        pthread_create(&consumidores[i], NULL, consumidor_hilo, NULL);
    }

    for (int i = 0; i < 3; i++) {
        pthread_join(productores[i], NULL);
        pthread_join(consumidores[i], NULL);
    }

    munmap(cola, sizeof(ColaMensajes));
    close(shm_fd);
    shm_unlink(NOMBRE_MEMORIA);

    return 0;
}