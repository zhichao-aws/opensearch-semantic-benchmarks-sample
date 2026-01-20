import json
import random
import time
import argparse
import sys
import boto3
from locust import User, task, between, events, run_single_user

# Global variables to be populated from command line args
ENDPOINT_NAME = ""
REGION = "us-west-2"
QUERIES = []

def load_queries(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

class SageMakerUser(User):
    wait_time = between(0.00001, 0.00005)  # Simulated think time

    def on_start(self):
        self.sm_runtime = boto3.client("sagemaker-runtime", region_name=REGION)
        self.query_index = 0

    @task
    def invoke_endpoint(self):
        if not QUERIES:
            return

        # Select a query (round-robin or random)
        # query = QUERIES[self.query_index % len(QUERIES)]
        # self.query_index += 1
        query = random.choice(QUERIES)

        # Prepare payload
        payload = json.dumps(query)
        
        start_time = time.time()
        try:
            resp = self.sm_runtime.invoke_endpoint(
                EndpointName=ENDPOINT_NAME,
                ContentType="application/json",
                Body=payload.encode("utf-8")
            )
            body = resp["Body"]
            data = body.read()
            body.close()

            total_time = int((time.time() - start_time) * 1000)
            
            # Report success to Locust
            events.request.fire(
                request_type="SageMaker",
                name="invoke_endpoint",
                response_time=total_time,
                response_length=0,
                exception=None,
            )
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="SageMaker",
                name="invoke_endpoint",
                response_time=total_time,
                response_length=0,
                exception=e,
            )

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--endpoint-name", type=str, env_var="ENDPOINT_NAME", help="SageMaker Endpoint Name")
    parser.add_argument("--region", type=str, default="us-west-2", env_var="AWS_REGION", help="AWS Region")
    parser.add_argument("--queries-file", type=str, default="tokenizer_server/queries.json", help="Path to queries.json")

@events.test_start.add_listener
def _(environment, **kwargs):
    global ENDPOINT_NAME, REGION, QUERIES
    # Locust arguments are available in environment.parsed_options
    if environment.parsed_options:
        if environment.parsed_options.endpoint_name:
            ENDPOINT_NAME = environment.parsed_options.endpoint_name
        if environment.parsed_options.region:
            REGION = environment.parsed_options.region
        
        queries_file = environment.parsed_options.queries_file
        try:
            QUERIES = load_queries(queries_file)
            print(f"Loaded {len(QUERIES)} queries from {queries_file}")
        except Exception as e:
            print(f"Error loading queries from {queries_file}: {e}")
            sys.exit(1)
    
    if not ENDPOINT_NAME:
        print("Error: --endpoint-name is required")
        sys.exit(1)

# This block allows running directly with python without 'locust' command if needed, 
# but typically you run this with `locust -f ...`
if __name__ == "__main__":
    # For debugging/single run purposes
    pass
