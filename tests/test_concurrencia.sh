#!/bin/bash
# Pruebas de funcionalidad básica: conexión, envío y consumo de mensajes
set -e

cd "$(dirname "$0")/../src"

BROKER_PORT=${BROKER_PORT:-4444}
./broker 4 32 $BROKER_PORT &
BROKER_PID=$!
sleep 1

cleanup() {
    kill $BROKER_PID 2>/dev/null
    killall -9 producer consumer 2>/dev/null || true
    rm -f /dev/shm/memoria_cola_mensajes
}
trap cleanup EXIT

# Conexión productor-broker
echo "mensaje test" | ./producer > prod.log
grep -q "OK" prod.log && echo "Conexión productor-broker: OK"

# Conexión consumidor-broker
(echo "grupo_test" | ./consumer > cons.log) &
sleep 1
grep -q "Mensaje #" cons.log && echo "Conexión consumidor-broker: OK"

# Envío de mensajes
for i in {1..3}; do echo "msg $i"; done | ./producer > prod2.log
grep -c "OK" prod2.log | grep -q 3 && echo "Envío de mensajes: OK"

# Consumo de mensajes
sleep 2
grep -c "Mensaje #" cons.log | grep -q 3 && echo "Consumo de mensajes: OK"

cleanup