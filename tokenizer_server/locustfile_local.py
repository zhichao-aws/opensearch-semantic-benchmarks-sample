import json
import random
import time
import argparse
import sys
from locust import HttpUser, task, between, events, run_single_user

# Global variables
QUERIES = []

def load_queries(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

class LocalTokenizerUser(HttpUser):
    wait_time = between(0.00001, 0.00005)  # Simulated think time

    @task
    def invoke_local(self):
        if not QUERIES:
            return

        query = random.choice(QUERIES)

        # Prepare payload - similar to readme example
        # The readme example sends an array of strings: '["This is sentence one", ...]'
        # or a single string (which is valid JSON string).
        # Assuming queries.json is a list of strings, we can send one query as a list or single string.
        # Based on readme: -d '["This is sentence one", "This is sentence two"]'
        
        # We will wrap the single query in a list to match the "batch" format,
        # or send it directly if the server supports single string.
        # Let's send it as a JSON list containing one string for consistency with typical batch APIs.
        payload = [query]
        
        headers = {"Content-Type": "application/json"}
        
        # POST to /invocations
        self.client.post("/invocations", json=payload, headers=headers)

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--queries-file", type=str, default="tokenizer_server/queries.json", help="Path to queries.json")

@events.test_start.add_listener
def _(environment, **kwargs):
    global QUERIES
    # Locust arguments are available in environment.parsed_options
    if environment.parsed_options:
        queries_file = environment.parsed_options.queries_file
        try:
            QUERIES = load_queries(queries_file)
            print(f"Loaded {len(QUERIES)} queries from {queries_file}")
        except Exception as e:
            print(f"Error loading queries from {queries_file}: {e}")
            sys.exit(1)

if __name__ == "__main__":
    pass
