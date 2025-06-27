# run with UI
# locust -f locust_benchmark_sm.py --use-case nlp --payload sample_payload.json --endpoint-name oc-0618-g4dn

# run with headless
export MAX_USERS=160
export SECOND_STAGE_DURATION=100
locust -f locust_benchmark_sm.py \
    --headless \
    --size-per-doc 50 \
    --request-size 50 \
    --endpoint-name oc-0618-g4dn \
    --json-file test