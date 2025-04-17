// consumer.c (modificado)
// - Lee todos los mensajes disponibles hasta que la cola está vacía
// - Muestra cada mensaje recibido

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PUERTO 4444
#define LONGITUD_MAXIMA_MENSAJE 256
#define IP_SERVIDOR "127.0.0.1"

typedef struct {
    int id;
    char contenido[LONGITUD_MAXIMA_MENSAJE];
    time_t timestamp;
} Mensaje;

int main() {
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
    recv(socket_cliente, &id_consumidor, sizeof(int), 0);

    int contador_mensajes = 0;
    while (1) {
        // Pedir un mensaje
        char peticion = 'R';
        send(socket_cliente, &peticion, 1, 0);
        send(socket_cliente, &id_consumidor, sizeof(int), 0);

        char buffer[sizeof(Mensaje) + 1];
        int bytes_recibidos = recv(socket_cliente, buffer, sizeof(buffer) - 1, 0);

        if (bytes_recibidos > 0) {
            if (bytes_recibidos == 6 && strncmp(buffer, "VACIO", 5) == 0) {
                printf("No hay mensajes, esperando...\n");
                sleep(1); // Espera 1 segundo antes de volver a pedir
                continue; // No termina, sigue pidiendo
            } else if (bytes_recibidos == sizeof(Mensaje)) {
                Mensaje *msg = (Mensaje*)buffer;
                contador_mensajes++;
                printf("Mensaje #%d recibido: %s\n", msg->id, msg->contenido);
            }
        } else {
            perror("Error al recibir datos");
            break;
        }
    }

    close(socket_cliente);
    printf("Total de mensajes leídos: %d\n", contador_mensajes);
    return 0;
}