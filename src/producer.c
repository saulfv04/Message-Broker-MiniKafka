// producer.c (modificado)
// - Contenido de mensaje quemado
// - No lee de stdin
// - Termina después de enviar un mensaje

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

int main(int argc, char *argv[]) {
    int id_mensaje = 0;
    if (argc > 1) {
        id_mensaje = atoi(argv[1]); // ID pasado como argumento
    }

    // Crear socket
    int socket_cliente = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_cliente == -1) {
        perror("Error al crear el socket");
        return 1;
    }

    // Configurar dirección del servidor
    struct sockaddr_in direccion_servidor;
    direccion_servidor.sin_family = AF_INET;
    direccion_servidor.sin_port = htons(PUERTO);
    
    if (inet_pton(AF_INET, IP_SERVIDOR, &direccion_servidor.sin_addr) <= 0) {
        perror("Dirección inválida");
        close(socket_cliente);
        return 1;
    }

    // Conectar al servidor
    if (connect(socket_cliente, (struct sockaddr*)&direccion_servidor, sizeof(direccion_servidor)) < 0) {
        perror("Error en la conexión");
        close(socket_cliente);
        return 1;
    }

    // Enviar identificador de productor
    char tipo = 'P';
    send(socket_cliente, &tipo, 1, 0);

    // Crear mensaje con contenido quemado
    Mensaje msg;
    msg.id = id_mensaje;
    msg.timestamp = time(NULL);
    sprintf(msg.contenido, "Mensaje automatizado #%d", id_mensaje);

    // Enviar mensaje
    if (send(socket_cliente, &msg, sizeof(Mensaje), 0) < 0) {
        perror("Error al enviar mensaje");
    } else {
        printf("Productor %d: mensaje enviado\n", id_mensaje);
    }

    // Esperar respuesta
    char respuesta[10];
    recv(socket_cliente, respuesta, sizeof(respuesta), 0);
    
    close(socket_cliente);
    return 0;
}