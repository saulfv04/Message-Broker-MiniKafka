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
    int contador_mensajes = 0;
    int cola_vacia = 0;
    
    while (!cola_vacia) {
        // Crear socket para cada solicitud
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

        // Enviar identificador de consumidor
        char tipo = 'C';
        send(socket_cliente, &tipo, 1, 0);

        // Recibir mensaje o notificación de cola vacía
        char buffer[sizeof(Mensaje) + 1];
        int bytes_recibidos = recv(socket_cliente, buffer, sizeof(buffer) - 1, 0);
        
        if (bytes_recibidos > 0) {
            // Verificar si es un mensaje o la señal de "VACIO"
            if (bytes_recibidos == 6 && strncmp(buffer, "VACIO", 5) == 0) {
                printf("No hay más mensajes disponibles.\n");
                cola_vacia = 1;
            } else if (bytes_recibidos == sizeof(Mensaje)) {
                // Convertir el buffer recibido a un mensaje
                Mensaje *msg = (Mensaje*)buffer;
                contador_mensajes++;
                
                printf("Mensaje #%d recibido: %s\n", msg->id, msg->contenido);
            }
        } else {
            perror("Error al recibir datos");
        }

        close(socket_cliente);
    }
    
    printf("Total de mensajes leídos: %d\n", contador_mensajes);
    return 0;
}