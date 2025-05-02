# Mini-Kafka: Sistema de Mensajería Distribuido en C

Este proyecto implementa un sistema de mensajería distribuido inspirado en Apache Kafka, desarrollado en lenguaje C. El sistema permite la comunicación entre múltiples **productores** y **consumidores**, gestionada a través de un **broker central**. Está diseñado para garantizar concurrencia, persistencia y distribución eficiente de mensajes, incluso en entornos multicliente.

## 🧱 Arquitectura General

- **Productores:** Generan mensajes y los envían al broker a través de sockets TCP.
- **Broker:** Proceso servidor que recibe conexiones de productores y consumidores, mantiene una cola de mensajes en memoria compartida, gestiona los offsets de lectura, realiza persistencia en disco y registra eventos de consumo.
- **Consumidores:** Clientes que solicitan mensajes al broker y los reciben uno a la vez. Pueden operar de forma independiente o agruparse para balancear el consumo mediante round-robin.
- **Persistencia:** Los mensajes se almacenan en un archivo de texto (persistencia_mensajes.txt) utilizando buffers dobles para escritura asíncrona y eficiente.
- **Log de Consumo:**  Cada vez que un consumidor recibe un mensaje, se registra el evento con detalles como el grupo, el consumidor y la hora en log_consumo.txt.
- **Limpieza:** Un hilo limpia mensajes ya leídos por todos los consumidores para evitar saturación de la cola.

## 📁 Estructura del Proyecto

```
/project-root
├── src/
│   ├── broker.c         # Lógica del broker (multithreading, socket, sincronización)
│   ├── producer.c       # Cliente productor (envía mensajes al broker)
│   ├── consumer.c       # Cliente consumidor (solicita mensajes al broker)
├── Makefile             # Script de compilación
├── README.md            # Documentación del sistema
```

## ⚙️ Instrucciones de Compilación y Ejecución

1. **Compilar:**
   ```bash
   make
   ```

2. **Ejecutar el broker:**
   ```bash
   ./broker
   ```

3. **Ejecutar un productor:**
   ```bash
   ./producer
   ```

4. **Ejecutar un consumidor:**
   ```bash
   ./consumer
   ```

> Todos los procesos deben ejecutarse en la misma máquina (localhost). El broker escucha por defecto en el puerto `4444`.

## 🔒 Estrategia de Concurrencia y Prevención de Interbloqueos

- Se utilizan **mutexes (`pthread_mutex_t`)** para proteger el acceso concurrente a la cola en memoria compartida.
- El acceso a estructuras globales como la cola de persistencia y los offsets también está sincronizado.
- Los mutexes están configurados para ser compartidos entre procesos (`PTHREAD_PROCESS_SHARED`), lo que facilita futuras extensiones.
- La comunicación entre hilos del broker (e.g., persistencia y limpieza) se realiza mediante **condiciones (`pthread_cond_t`)** para evitar espera activa.
- Se emplean semáforos para bloquear productores cuando la cola está llena y para notificar consumidores cuando hay mensajes nuevos disponibles.

## 🔁 Flujo del Sistema

1. Un productor se conecta al broker y envía un mensaje.
2. El broker almacena el mensaje en una cola circular de memoria compartida.
3. El mensaje también se coloca en un buffer de persistencia, que es escrito de forma asíncrona al archivo persistencia_mensajes.txt por un hilo dedicado.
4. Un consumidor solicita un mensaje y el broker le responde con el siguiente mensaje según el offset del grupo al que pertenece.
5. Se registra un evento en el buffer de log de consumo, que luego es volcado al archivo log_consumo.txt.
6. Si todos los grupos han leído un mensaje, este se elimina de la cola para liberar espacio.

## 👥 Soporte de Grupos de Consumidores

- Cada grupo tiene un nombre, un offset global y una lista de consumidores activos.
- El broker asigna mensajes a los consumidores en orden round-robin.
- El offset de grupo avanza solo cuando el mensaje ha sido leído por el consumidor correspondiente.

## 💾 Mecanismo de Persistencia y Logs

## Persistencia de Mensajes

El broker utiliza dos buffers (buffer_a y buffer_b) que se alternan para almacenar mensajes antes de escribirlos en disco.

Cuando un buffer se llena o después de un intervalo periódico, los buffers se intercambian y el hilo de persistencia escribe el contenido en persistencia_mensajes.txt.

Este diseño evita bloqueos entre los hilos productores y el hilo que escribe a disco.

## Log de Consumo

Cada vez que un consumidor recibe un mensaje, se crea un registro que incluye:
- ID del mensaje
- Socket del productor original
- Nombre del grupo
- ID del consumidor
- Timestamp de lectura

Al igual que la persistencia, se usan buffers dobles para registrar estos eventos de forma asíncrona y eficiente.
El archivo resultante es log_consumo.txt, que sirve para auditoría y análisis del sistema.

##  👥 Gestión de Grupos de Consumidores

Al conectarse, un consumidor es automáticamente asignado al grupo con menos carga (menos consumidores).
Cada grupo mantiene su propio offset global, el cual avanza solo cuando uno de sus consumidores ha leído exitosamente un mensaje.
Los mensajes se distribuyen de manera balanceada entre los consumidores del grupo usando round-robin.
Se previene que múltiples consumidores lean el mismo mensaje gracias a la sincronización con mutex por grupo.

## 🧪 Pruebas Implementadas

- Simulación de múltiples hilos productores y consumidores dentro del broker para validar concurrencia.
- Envío/recepción de mensajes por sockets para validar la comunicación.
- Persistencia en archivo y limpieza automatizada confirmadas con logs.

## ⚠️ Limitaciones Conocidas

- No se implementa recuperación automática del estado ante reinicio (aunque los mensajes persisten en log).
- No hay autenticación de clientes.
- Las conexiones se asumen como locales; no se ha probado en red distribuida real.


