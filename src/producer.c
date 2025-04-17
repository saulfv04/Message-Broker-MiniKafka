// producer.c (modificado)
// - Contenido de mensaje leído de stdin
// - Termina cuando el usuario escribe 'salir'

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

    // Identificarse como productor
    char tipo = 'P';
    send(socket_cliente, &tipo, 1, 0);

    int id = 1;
    char buffer[16];
    while (1) {
        Mensaje m;
        m.id = id++;
        printf("Escribe el mensaje a enviar (o 'salir' para terminar): ");
        fgets(m.contenido, LONGITUD_MAXIMA_MENSAJE, stdin);
        m.contenido[strcspn(m.contenido, "\n")] = 0; // quitar salto de línea
        m.timestamp = time(NULL);

        if (strcmp(m.contenido, "salir") == 0) break;

        send(socket_cliente, &m, sizeof(Mensaje), 0);

        int bytes = recv(socket_cliente, buffer, sizeof(buffer)-1, 0);
        if (bytes > 0) {
            buffer[bytes] = 0;
            printf("Respuesta del broker: %s\n", buffer);
        } else {
            printf("Broker desconectado.\n");
            break;
        }
    }

    close(socket_cliente);
    return 0;
}