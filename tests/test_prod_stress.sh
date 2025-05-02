#!/bin/bash

NUM_PROD=100
BATCH=100

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
PRODUCER_BIN="$PROJECT_ROOT/src/producer"

echo "Iniciando $NUM_PROD productores en lotes de $BATCH..."

i=1
while [ $i -le $NUM_PROD ]; do
    for ((j=0; j<BATCH && i<=NUM_PROD; j++, i++)); do
        "$PRODUCER_BIN" > /dev/null 2>&1 &
    done
    wait
done

echo "Test de productores masivos terminado."