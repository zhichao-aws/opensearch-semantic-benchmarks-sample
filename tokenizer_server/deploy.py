import time
import boto3
import argparse
from botocore.exceptions import ClientError


def wait_for_endpoint_in_service(sm_client, endpoint_name: str, poll_sec: int = 20, timeout_sec: int = 30 * 60):
    """Wait until endpoint is InService or fails."""
    start = time.time()
    while True:
        resp = sm_client.describe_endpoint(EndpointName=endpoint_name)
        status = resp["EndpointStatus"]
        print(f"[wait] endpoint={endpoint_name} status={status}")

        if status == "InService":
            return resp

        if status in ("Failed", "OutOfService"):
            reason = resp.get("FailureReason", "Unknown")
            raise RuntimeError(f"Endpoint {endpoint_name} entered status={status}. Reason: {reason}")

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Timed out waiting for endpoint {endpoint_name} to be InService.")

        time.sleep(poll_sec)


def parse_args():
    parser = argparse.ArgumentParser(description="Deploy Tokenizer Server to SageMaker")
    parser.add_argument("--region", type=str, default="us-west-2", help="AWS Region (default: us-west-2)")
    parser.add_argument("--image-uri", type=str, required=True, help="Full ECR Image URI")
    parser.add_argument("--execution-role-arn", type=str, required=True, help="SageMaker Execution Role ARN")
    parser.add_argument("--instance-type", type=str, default="ml.m5.large", help="Instance type (default: ml.m5.large)")
    parser.add_argument("--initial-instance-count", type=int, default=1, help="Initial instance count (default: 1)")
    return parser.parse_args()


def main():
    args = parse_args()
    
    region = args.region
    image_uri = args.image_uri
    execution_role_arn = args.execution_role_arn
    instance_type = args.instance_type
    initial_instance_count = args.initial_instance_count

    # Resource names
    ts = int(time.time())
    model_name = f"hf-tokenizer-model-{ts}"
    endpoint_config_name = f"hf-tokenizer-epc-{ts}"
    endpoint_name = f"hf-tokenizer-ep-{ts}"

    sm = boto3.client("sagemaker", region_name=region)

    # ----------------------------
    # 1) CreateModel (custom container)
    # ----------------------------
    # Environment lets you control tokenizer selection and worker count.
    # Your container defaults to using all cores via nproc; WORKERS is optional.
    primary_container = {
        "Image": image_uri,
        "Environment": {},
    }

    try:
        sm.create_model(
            ModelName=model_name,
            ExecutionRoleArn=execution_role_arn,
            PrimaryContainer=primary_container,
        )
        print(f"[ok] created model: {model_name}")
    except ClientError as e:
        raise RuntimeError(f"CreateModel failed: {e}")

    # ----------------------------
    # 2) CreateEndpointConfig
    # ----------------------------
    # This defines the instance type/count for hosting.
    try:
        sm.create_endpoint_config(
            EndpointConfigName=endpoint_config_name,
            ProductionVariants=[
                {
                    "VariantName": "AllTraffic",
                    "ModelName": model_name,
                    "InstanceType": instance_type,
                    "InitialInstanceCount": initial_instance_count,
                }
            ],
        )
        print(f"[ok] created endpoint config: {endpoint_config_name}")
    except ClientError as e:
        raise RuntimeError(f"CreateEndpointConfig failed: {e}")

    # ----------------------------
    # 3) CreateEndpoint
    # ----------------------------
    # This provisions the endpoint and deploys the container.
    try:
        sm.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=endpoint_config_name,
        )
        print(f"[ok] creating endpoint: {endpoint_name}")
    except ClientError as e:
        raise RuntimeError(f"CreateEndpoint failed: {e}")

    # ----------------------------
    # 4) Wait until InService
    # ----------------------------
    wait_for_endpoint_in_service(sm, endpoint_name)
    print(f"[ok] endpoint is InService: {endpoint_name}")

    print("\nDeployment complete.")
    print(f"EndpointName = {endpoint_name}")
    print(f"ModelName = {model_name}")
    print(f"EndpointConfigName = {endpoint_config_name}")


if __name__ == "__main__":
    main()
