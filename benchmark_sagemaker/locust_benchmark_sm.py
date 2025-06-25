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

load_dotenv()

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
    根据指定的文档大小和请求大小生成payload
    """
    # 读取doc.txt
    doc_path = Path(__file__).parent / "doc.txt"
    with doc_path.open("r", encoding="utf-8") as f:
        doc_content = f.read()

    # 计算目标字节数
    size_per_doc_bytes = size_per_doc_kb * 1024
    request_size_bytes = request_size_kb * 1024

    # 调整单个文档大小
    doc_content_bytes = doc_content.encode("utf-8")
    current_size = len(doc_content_bytes)

    if current_size < size_per_doc_bytes:
        # 需要重复内容
        repeat_count = (size_per_doc_bytes + current_size - 1) // current_size
        adjusted_doc = doc_content * repeat_count
        adjusted_doc = adjusted_doc.encode("utf-8")[:size_per_doc_bytes].decode(
            "utf-8", errors="ignore"
        )
    else:
        # 需要截断内容
        adjusted_doc = doc_content_bytes[:size_per_doc_bytes].decode(
            "utf-8", errors="ignore"
        )

    # 计算需要的文档数量
    doc_count = request_size_kb // size_per_doc_kb
    if request_size_kb % size_per_doc_kb != 0:
        raise ValueError(
            f"request_size ({request_size_kb}KB) 必须能被 size_per_doc ({size_per_doc_kb}KB) 整除"
        )

    # 创建文档列表
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

        # 生成payload
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
    stages = [
        {"duration": 5, "users": 10, "spawn_rate": 5},
        {"duration": 30, "users": 10, "spawn_rate": 10},
    ]

    def tick(self):
        run_time = self.get_run_time()

        for stage in self.stages:
            if run_time < stage["duration"]:
                tick_data = (stage["users"], stage["spawn_rate"])
                return tick_data

        return None
