#!/bin/bash

NUM_CONS=2000
BATCH=2000

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
CONSUMER_BIN="$PROJECT_ROOT/src/consumer"

echo "Iniciando $NUM_CONS consumidores en lotes de $BATCH..."

i=1
while [ $i -le $NUM_CONS ]; do
    for ((j=0; j<BATCH && i<=NUM_CONS; j++, i++)); do
        "$CONSUMER_BIN" > /dev/null 2>&1 &
    done
    sleep 0.2  # Espera 200ms entre lotes (ajusta si quieres)
done

wait

echo "Test de consumidores masivos terminado."