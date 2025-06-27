#!/bin/bash

ENDPOINT_NAME="oc-0618-g4dn"
PARAMS="[(1,5),(2,5),(5,5),(10,5)]"

python3 automated_benchmark.py \
    --endpoint-name "$ENDPOINT_NAME" \
    --params "$PARAMS" \
    --start-users 4 \
    --step-size 4 \
    --run-time 900 \
    --output "performance_results_$(date +%Y%m%d_%H%M%S).csv"