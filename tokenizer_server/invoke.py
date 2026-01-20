import json
import boto3
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Invoke SageMaker Endpoint")
    parser.add_argument("--endpoint-name", type=str, required=True, help="SageMaker Endpoint Name")
    parser.add_argument("--region", type=str, default="us-west-2", help="AWS Region (default: us-west-2)")
    parser.add_argument("--payload", type=str, default="hello from sagemaker", help="Input text payload")
    return parser.parse_args()


def main():
    args = parse_args()

    endpoint_name = args.endpoint_name
    region = args.region
    payload = args.payload

    rt = boto3.client("sagemaker-runtime", region_name=region)

    print(f"Invoking endpoint: {endpoint_name}")
    print(f"Payload: {payload}")

    try:
        resp = rt.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType="application/json",
            Body=json.dumps(payload).encode("utf-8"),
        )
        
        out = json.loads(resp["Body"].read().decode("utf-8"))
        print("\nResponse:")
        print(json.dumps(out, indent=2))
        
    except Exception as e:
        print(f"\nError invoking endpoint: {e}")


if __name__ == "__main__":
    main()
