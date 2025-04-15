#!/bin/bash
# test_broker.sh - Script para probar el broker con múltiples productores

TOTAL_PRODUCTORES=100
PUERTO=8080

# Función para limpiar todo antes de iniciar
cleanup() {
    echo "Limpiando procesos y recursos anteriores..."
    # Matar procesos existentes
    killall -9 broker 2>/dev/null
    killall -9 producer 2>/dev/null
    killall -9 consumer 2>/dev/null
    
    # Esperar a que todo termine
    sleep 1
    
    # Limpiar memoria compartida
    rm -f /dev/shm/memoria_cola_mensajes
    
    # Asegurarse que el puerto esté libre
    fuser -k ${PUERTO}/tcp 2>/dev/null
    
    # Esperar un momento para que el sistema libere recursos
    sleep 2
}

# Función para limpiar al salir
cleanup_exit() {
    echo "Deteniendo procesos..."
    killall -9 broker 2>/dev/null
    killall -9 producer 2>/dev/null
    killall -9 consumer 2>/dev/null
    rm -f /dev/shm/memoria_cola_mensajes
    exit 0
}

# Establecer manejador para señales de terminación
trap cleanup_exit SIGINT SIGTERM

# Limpiar al inicio
cleanup

# Compilar los programas
echo "Compilando broker, producer y consumer..."
gcc -o broker broker.c -pthread
gcc -o producer producer.c
gcc -o consumer consumer.c

# Iniciar el broker en segundo plano
echo "Iniciando el broker..."
./broker &
BROKER_PID=$!

# Esperar a que el broker esté listo
echo "Esperando a que el broker esté listo..."

# Crear productores en paralelo, pero en grupos más pequeños
echo "Iniciando $TOTAL_PRODUCTORES productores..."
for i in $(seq 1 $TOTAL_PRODUCTORES); do
    ./producer $i &
    
    # Pequeña pausa para evitar saturar el sistema
    if [ $((i % 50)) -eq 0 ]; then
        echo "Productores lanzados: $i/$TOTAL_PRODUCTORES"
    fi
done

# Esperar a que todos los procesos en segundo plano terminen
echo "Esperando a que los productores terminen..."
wait $(jobs -p | grep -v $BROKER_PID)

# Ejecutar el consumidor para leer todos los mensajes
echo "Iniciando consumidor para leer todos los mensajes..."
./consumer

# Detener el broker y limpiar
echo "Deteniendo el broker..."
kill $BROKER_PID
cleanup

echo "Prueba completada."