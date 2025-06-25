# run with UI
# locust -f locust_benchmark_sm.py --use-case nlp --payload sample_payload.json --endpoint-name oc-0618-g4dn

# run with headless
# locust -f locust_benchmark_sm.py \
#     --users 10 \
#     --spawn-rate 5 \
#     --run-time 30s \
#     --headless \
#     --size-per-doc 1 \
#     --request-size 10 \
#     --endpoint-name oc-0618-g4dn