// consumer.c (modificado)
// - Lee todos los mensajes disponibles hasta que la cola está vacía
// - Muestra cada mensaje recibido

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>

#define PUERTO 4444
#define LONGITUD_MAXIMA_MENSAJE 256
#define IP_SERVIDOR "127.0.0.1"
#define LONGITUD_NOMBRE_GRUPO 32

typedef struct {
    int id;
    char contenido[LONGITUD_MAXIMA_MENSAJE];
    time_t timestamp;
} Mensaje;

// Variable global para terminar
volatile sig_atomic_t terminar = 0;

// Manejador de señal
void handler(int sig) {
    terminar = 1;
}

int main() {
    signal(SIGINT, handler);
    signal(SIGTERM, handler);

    int socket_cliente = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_cliente == -1) {
        perror("Error al crear el socket");
        return 1;
    }

    struct sockaddr_in direccion_servidor;
    direccion_servidor.sin_family = AF_INET;
    direccion_servidor.sin_port = htons(PUERTO);

    if (inet_pton(AF_INET, IP_SERVIDOR, &direccion_servidor.sin_addr) <= 0) {
        perror("Dirección inválida");
        close(socket_cliente);
        return 1;
    }

    if (connect(socket_cliente, (struct sockaddr*)&direccion_servidor, sizeof(direccion_servidor)) < 0) {
        perror("Error en la conexión");
        close(socket_cliente);
        return 1;
    }

    // Identificarse como consumidor
    char tipo = 'C';
    send(socket_cliente, &tipo, 1, 0);

    // Recibir el id asignado por el broker
    int id_consumidor;
    int bytes = recv(socket_cliente, &id_consumidor, sizeof(int), 0);
    if (bytes != sizeof(int)) {
        char buffer[32];
        recv(socket_cliente, buffer, sizeof(buffer)-1, 0);
        buffer[sizeof(buffer)-1] = 0;
        printf("Error del broker: %s\n", buffer);
        close(socket_cliente);
        return 1;
    }

    printf("ID de consumidor recibido: %d\n", id_consumidor);

    int contador_mensajes = 0;
    while (!terminar) {
        printf("Pidiendo mensaje...\n");
        char peticion = 'R';
        send(socket_cliente, &peticion, 1, 0);
        send(socket_cliente, &id_consumidor, sizeof(int), 0);

        char buffer[sizeof(Mensaje) + 1];

        fd_set readfds;
        struct timeval tv;
        FD_ZERO(&readfds);
        FD_SET(socket_cliente, &readfds);
        tv.tv_sec = 0;
        tv.tv_usec = 200000; // 200 ms

        int ready = select(socket_cliente + 1, &readfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (terminar) break;
            perror("select");
            break;
        }
        if (ready == 0) continue; // Timeout, vuelve a chequear terminar

        if (FD_ISSET(socket_cliente, &readfds)) {
            int bytes_recibidos = recv(socket_cliente, buffer, sizeof(buffer) - 1, 0);
            printf("Bytes recibidos: %d\n", bytes_recibidos);

            if (bytes_recibidos > 0) {
                Mensaje *msg = (Mensaje*)buffer;
                if (msg->id == -1 || strcmp(msg->contenido, "VACIO") == 0) {
                    // Mensaje vacío, no imprimir nada
                    usleep(100000); // Espera 100ms antes de volver a pedir
                    continue;
                } else if (bytes_recibidos == sizeof(Mensaje)) {
                    contador_mensajes++;
                    printf("Mensaje #%d recibido: %s\n", msg->id, msg->contenido);
                }
            } else {
                perror("Error al recibir datos");
                break;
            }
        }
    }

    close(socket_cliente);
    printf("\nTotal de mensajes leídos: %d\n", contador_mensajes);
    printf("Consumidor terminado limpiamente.\n");
    return 0;
}