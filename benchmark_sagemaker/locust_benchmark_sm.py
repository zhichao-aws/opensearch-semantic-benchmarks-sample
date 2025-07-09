import boto3
from botocore.config import Config
import os
import sys
import json
from locust import User, task, between, events, LoadTestShape

import numpy as np
from PIL import Image
from pathlib import Path
import random
import time
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# How to use
# 1. install locust & boto3
#   pip install locust boto3
# 2. run benchmark via cli
# with UI
# Since we are using a custom client for the request we need to define the "Host" as -.
#   locust -f locust_benchmark_sm.py --size-per-doc 1 --request-size 10 --endpoint-name your-endpoint-name
#
# headless
# --users  Number of concurrent Locust users
# --spawn-rate  The rate per second in which users are spawned until num users
# --run-time duration of test
#   locust -f locust_benchmark_sm.py \
#       --users 60 \
#       --spawn-rate 1 \
#       --run-time 360s \
#       --headless \
#       --size-per-doc 1 \
#       --request-size 10 \
#       --endpoint-name your-endpoint-name

content_type = "application/json"


def generate_payload_from_doc(size_per_doc_kb, request_size_kb):
    """
    Generate a payload according to the given document size and total request size.
    """
    # Read doc.txt
    doc_path = Path(__file__).parent / "doc.txt"
    with doc_path.open("r", encoding="utf-8") as f:
        doc_content = f.read()

    # Calculate target byte counts
    size_per_doc_bytes = size_per_doc_kb * 1024
    request_size_bytes = request_size_kb * 1024

    # Adjust single document size
    doc_content_bytes = doc_content.encode("utf-8")
    current_size = len(doc_content_bytes)

    if current_size < size_per_doc_bytes:
        # Repeat content if needed
        repeat_count = (size_per_doc_bytes + current_size - 1) // current_size
        adjusted_doc = doc_content * repeat_count
        adjusted_doc = adjusted_doc.encode("utf-8")[:size_per_doc_bytes].decode(
            "utf-8", errors="ignore"
        )
    else:
        # Truncate content if needed
        adjusted_doc = doc_content_bytes[:size_per_doc_bytes].decode(
            "utf-8", errors="ignore"
        )

    # Compute required number of documents
    doc_count = request_size_kb // size_per_doc_kb
    if request_size_kb % size_per_doc_kb != 0:
        raise ValueError(
            f"request_size ({request_size_kb}KB) must be divisible by size_per_doc ({size_per_doc_kb}KB)"
        )

    # Create the document list
    payload_list = [adjusted_doc] * doc_count

    return json.dumps(payload_list)


class SageMakerClient:
    def __init__(self):
        super().__init__()

        self.session = boto3.Session()
        self.payload = globals()["payload"]

        self.client = self.session.client("sagemaker-runtime")
        self.content_type = content_type

    def send(self, endpoint_name):

        request_meta = {
            "request_type": "InvokeEndpoint",
            "name": endpoint_name,
            "start_time": time.time(),
            "response_length": 0,
            "response": None,
            "context": {},
            "exception": None,
        }

        start_perf_counter = time.perf_counter()

        try:
            response = self.client.invoke_endpoint(
                EndpointName=endpoint_name,
                Body=self.payload,
                ContentType=self.content_type,
            )
            # print(response)
        except Exception as e:
            request_meta["exception"] = e

        request_meta["response_time"] = (
            time.perf_counter() - start_perf_counter
        ) * 1000

        events.request.fire(**request_meta)
        if request_meta["exception"] is not None:
            logger.error(
                f"Error invoking endpoint {endpoint_name}: {request_meta['exception']}"
            )


class SageMakerUser(User):
    abstract = True

    @events.init_command_line_parser.add_listener
    def _(parser):
        parser.add_argument(
            "--endpoint-name",
            type=str,
            help="sagemaker endpoint you want to invoke",
            required=True,
        )
        parser.add_argument(
            "--size-per-doc", type=int, help="size per document in KB", required=True
        )
        parser.add_argument(
            "--request-size", type=int, help="total request size in KB", required=True
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        size_per_doc_kb = self.environment.parsed_options.size_per_doc
        request_size_kb = self.environment.parsed_options.request_size

        # Generate payload
        globals()["payload"] = generate_payload_from_doc(
            size_per_doc_kb, request_size_kb
        )

        self.client = SageMakerClient()


class SimpleSendRequest(SageMakerUser):
    wait_time = between(0.05, 0.5)

    @task
    def send_request(self):
        endpoint_name = self.environment.parsed_options.endpoint_name

        self.client.send(endpoint_name)


class StagesShape(LoadTestShape):
    """Two-stage load model:

    Stage 1: fixed 5-second warm-up, ramp to --max-users at --spawn-rate.
    Stage 2: maintain the same concurrency for --second-stage-duration seconds.
    """

    def __init__(self):
        # NOTE: LoadTestShape.__init__ takes no arguments
        super().__init__()

        # Stages will be built lazily once the environment (and its parsed_options) is available
        self._stages_built = False

    def _build_stages(self):
        """Create the stages configuration from environment variables."""
        max_users = int(os.environ.get("MAX_USERS", "10"))
        second_stage_duration = int(os.environ.get("SECOND_STAGE_DURATION", "30"))

        # Make durations cumulative (Locust expects duration to be cumulative seconds since start)
        self.stages = [
            {"duration": 5, "users": max_users, "spawn_rate": 100},
            {
                "duration": 5 + second_stage_duration,
                "users": max_users,
                "spawn_rate": 10,
            },
        ]
        self._stages_built = True

    def tick(self):
        # Lazily build stages when environment is ready
        if not self._stages_built:
            self._build_stages()

        run_time = self.get_run_time()

        for stage in self.stages:
            if run_time < stage["duration"]:
                return stage["users"], stage["spawn_rate"]

        return None
