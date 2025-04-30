#!/bin/bash
set -e

cd "$(dirname "$0")/../src"

BROKER_PORT=4444

./broker > broker.log 2>&1 &
sleep 1
./producer > producer.log 2>&1 &
sleep 1
./consumer > consumer.log 2>&1 &

BROKER_PID=$!
sleep 1

cleanup() {
    kill $BROKER_PID 2>/dev/null || true
    kill $CONSUMER_PID 2>/dev/null || true
    killall -9 producer consumer broker 2>/dev/null || true
    rm -f /dev/shm/memoria_cola_mensajes
}
trap cleanup EXIT

valgrind --leak-check=full --track-origins=yes --log-file=valgrind_producer.log ./producer > prod.log 2>&1

valgrind --leak-check=full --track-origins=yes --log-file=valgrind_consumer.log ./consumer > cons.log 2>&1 &
CONSUMER_PID=$!

# Espera máximo 5 segundos, luego mata todo
timeout=5
while kill -0 $CONSUMER_PID 2>/dev/null && [ $timeout -gt 0 ]; do
    sleep 1
    timeout=$((timeout-1))
done

kill $CONSUMER_PID 2>/dev/null || true
kill $BROKER_PID 2>/dev/null || true

# No uses wait aquí, cleanup se encarga de limpiar todo

echo "==== Valgrind broker ===="
cat valgrind_broker.log | grep -E "definitely lost|indirectly lost|still reachable|ERROR SUMMARY"

echo "==== Valgrind producer ===="
cat valgrind_producer.log | grep -E "definitely lost|indirectly lost|still reachable|ERROR SUMMARY"

echo "==== Valgrind consumer ===="
cat valgrind_consumer.log | grep -E "definitely lost|indirectly lost|still reachable|ERROR SUMMARY"

echo "==== Mensajes recibidos por el consumidor ===="
cat cons.log | grep "Mensaje #"
