#!/bin/bash

BROKER_HOST=127.0.0.1
BROKER_PORT=4444
NUM_PROD=1000       # Número de productores simultáneos

mkdir -p tests/logs_test_prod_stress

echo "Iniciando $NUM_PROD productores en paralelo..."

for ((i=1; i<=NUM_PROD; i++)); do
    ./src/producer > tests/logs_test_prod_stress/prod_$i.log 2>&1 &
done

wait

echo "Test de productores masivos terminado."