#!/bin/bash

NUM_CONS=1000         # Número de consumidores simultáneos

mkdir -p tests/logs_test_cons_stress

echo "Iniciando $NUM_CONS consumidores en paralelo..."

for ((i=1; i<=NUM_CONS; i++)); do
    ./src/consumer > tests/logs_test_cons_stress/cons_$i.log 2>&1 &
done

wait

echo "Test de consumidores masivos terminado."