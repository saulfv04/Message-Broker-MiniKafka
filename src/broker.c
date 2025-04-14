#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LONGITUD_MAXIMA_MENSAJE 256
#define LONGITUD_MAXIMA_MENSAJES 10

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
    ColaMensajes cola;
    inicializar_cola(&cola);

    // Insertar 3 mensajes
    for (int i = 1; i <= 11; i++) {
        Mensaje m;
        m.id = i;
        snprintf(m.contenido, LONGITUD_MAXIMA_MENSAJE, "Este es el mensaje número %d", i);
        m.timestamp = time(NULL);
        insertar_mensaje(&cola, m);
    }

    // Consumir los mensajes
    for (int i = 0; i < 11; i++) {
        Mensaje recibido;
        if (consumir_mensaje(&cola, &recibido) == 0) {
            imprimir_mensaje(&recibido);
        }
    }

    return 0;
}
