# Mini-Kafka: Sistema de Mensajería Distribuido en C

Este proyecto implementa un sistema de mensajería distribuido inspirado en Apache Kafka, desarrollado en lenguaje C. El sistema permite la comunicación entre múltiples **productores** y **consumidores**, gestionada a través de un **broker central**. Está diseñado para garantizar concurrencia, persistencia y distribución eficiente de mensajes, incluso en entornos multicliente.

## 🧱 Arquitectura General

- **Productores:** Generan mensajes y los envían al broker a través de sockets TCP.
- **Broker:** Administra la cola de mensajes en memoria compartida, gestiona conexiones y persistencia, y reparte los mensajes.
- **Consumidores:** Solicitan mensajes al broker. Pueden operar de forma independiente o en **grupos**, donde los mensajes se reparten con un algoritmo Round-Robin.
- **Persistencia:** Un hilo especial del broker escribe los mensajes en un log (`log_mensajes.txt`) de forma asíncrona.
- **Limpieza:** Un hilo limpia mensajes ya leídos por todos los consumidores para evitar saturación de la cola.

## 📁 Estructura del Proyecto

```
/project-root
│── src/
│   ├── broker.c         # Lógica del broker
│   ├── producer.c       # Cliente productor
│   ├── consumer.c       # Cliente consumidor
│── README.md            # Este archivo
│── Makefile             # Compilación
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

## 🔁 Flujo del Sistema

1. Productor se conecta → Envía mensaje al broker.
2. Broker guarda en cola (memoria compartida) → También encola para persistencia.
3. Hilo de persistencia lo guarda en archivo.
4. Consumidor se conecta → Solicita mensaje → Broker responde según su offset.
5. Offset se actualiza → Se dispara limpieza si todos los consumidores han leído.
6. En modo grupo: Round-robin garantiza balanceo de carga.

## 👥 Soporte de Grupos de Consumidores

- Cada grupo tiene un nombre, un offset global y una lista de consumidores activos.
- El broker asigna mensajes a los consumidores en orden round-robin.
- El offset de grupo avanza solo cuando el mensaje ha sido leído por el consumidor correspondiente.

## 🧪 Pruebas Implementadas

- Simulación de múltiples hilos productores y consumidores dentro del broker para validar concurrencia.
- Envío/recepción de mensajes por sockets para validar la comunicación.
- Persistencia en archivo y limpieza automatizada confirmadas con logs.

## ⚠️ Limitaciones Conocidas

- No se implementa recuperación automática del estado ante reinicio (aunque los mensajes persisten en log).
- No hay autenticación de clientes.
- Las conexiones se asumen como locales; no se ha probado en red distribuida real.

## 🧹 Posibles Mejoras

- Implementar mecanismo de recuperación de mensajes desde log.
- Mejorar tolerancia a fallos (heartbeat, reintentos).
- Añadir interfaz de monitoreo.
- Extender para que productores/consumidores puedan correr en máquinas distintas.
