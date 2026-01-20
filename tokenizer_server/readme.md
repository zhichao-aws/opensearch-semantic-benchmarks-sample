## build docker image

```bash
cd docker
docker build -t sagemaker-aiohttp-tokenizer:latest .
```

try run the server at local to verify:
```bash
docker run --rm -p 8080:8080 --cpuset-cpus="0,1" sagemaker-aiohttp-tokenizer:latest
curl -X POST http://localhost:8080/invocations \
     -H "Content-Type: application/json" \
     -d '["This is sentence one", "This is sentence two"]'

curl -X POST http://localhost:8080/invocations \
     -H "Content-Type: application/json" \
     -d '"single sentence"'
```

---

## Push your local Docker image to Amazon ECR

### 0) Prereqs

* AWS CLI configured (`aws configure` or `AWS_PROFILE=...`)
* Your image exists locally (example: `sagemaker-aiohttp-tokenizer:latest`)
* Choose a region (must match your SageMaker endpoint region)

```bash
export AWS_REGION=us-west-2
export IMAGE_LOCAL_NAME=sagemaker-aiohttp-tokenizer:latest
export ECR_REPO_NAME=hf-tokenizer-aiohttp
export IMAGE_TAG=v1
```

### 1) Get your AWS Account ID

```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo $AWS_ACCOUNT_ID
```

### 2) Create the ECR repository (one-time)

```bash
aws ecr create-repository \
  --repository-name $ECR_REPO_NAME \
  --region $AWS_REGION
```

If it already exists, AWS will error—safe to ignore.

### 3) Authenticate Docker to ECR

```bash
aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
```

### 4) Tag your local image with the ECR URI

```bash
export ECR_IMAGE_URI=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:${IMAGE_TAG}

docker tag ${IMAGE_LOCAL_NAME} ${ECR_IMAGE_URI}
```

### 5) Push the image

```bash
docker push ${ECR_IMAGE_URI}
```

### 6) Confirm it exists in ECR

```bash
aws ecr describe-images \
  --repository-name $ECR_REPO_NAME \
  --region $AWS_REGION
```

---

## B) Deploy the custom ECR image to a SageMaker Endpoint (boto3)

To deploy the image you just pushed, use the `deploy.py` script. You'll need a SageMaker Execution Role ARN.

```bash
# Define your SageMaker Execution Role (must allow ECR pull + CloudWatch logs)
export SAGEMAKER_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/service-role/AmazonSageMaker-ExecutionRole-20231110T135396"

python deploy.py \
  --region $AWS_REGION \
  --image-uri $ECR_IMAGE_URI \
  --execution-role-arn $SAGEMAKER_ROLE_ARN
```

---

## C) Deploy using AWS CLI (Alternative)

If you prefer using the AWS CLI instead of Python `boto3`, you can follow these steps. Ensure `$SAGEMAKER_ROLE_ARN` and `$ECR_IMAGE_URI` are set (see previous sections).

### 1) Prepare variables

```bash
export TS=$(date +%s)
export MODEL_NAME="hf-tokenizer-model-${TS}"
export CONFIG_NAME="hf-tokenizer-epc-${TS}"
export ENDPOINT_NAME="hf-tokenizer-ep-${TS}"
export INSTANCE_TYPE="ml.m5.large"
```

### 2) Create Model

```bash
aws sagemaker create-model \
    --model-name $MODEL_NAME \
    --execution-role-arn $SAGEMAKER_ROLE_ARN \
    --primary-container Image=$ECR_IMAGE_URI \
    --region $AWS_REGION
```

### 3) Create Endpoint Config

```bash
aws sagemaker create-endpoint-config \
    --endpoint-config-name $CONFIG_NAME \
    --production-variants VariantName=AllTraffic,ModelName=$MODEL_NAME,InstanceType=$INSTANCE_TYPE,InitialInstanceCount=1 \
    --region $AWS_REGION
```

### 4) Create Endpoint

```bash
aws sagemaker create-endpoint \
    --endpoint-name $ENDPOINT_NAME \
    --endpoint-config-name $CONFIG_NAME \
    --region $AWS_REGION
```

### 5) Wait for Endpoint to be InService

```bash
echo "Waiting for endpoint $ENDPOINT_NAME to be InService..."
aws sagemaker wait endpoint-in-service \
    --endpoint-name $ENDPOINT_NAME \
    --region $AWS_REGION
echo "Endpoint is ready!"
```

---

## D) Invoke the Endpoint (boto3)

After the endpoint is InService, you can test it with `invoke.py`.

```bash
python invoke.py \
  --endpoint-name $ENDPOINT_NAME \
  --region $AWS_REGION \
  --payload "hello from sagemaker"
```

---

## E) Benchmark with Locust

### 1) Test SageMaker Endpoint

You can use `locust` to benchmark the endpoint performance.

```bash
# Install locust if needed
pip install locust boto3

# Run benchmark (e.g. 10 users, spawn 1/sec, run for 20s)
locust -f locustfile.py \
  --headless \
  --run-time 20s \
  --endpoint-name $ENDPOINT_NAME \
  --region $AWS_REGION \
  --queries-file queries.json \
  --users 4 \
  --processes 10 \
  --spawn-rate 20
```

### 2) Test Local Server

If you are running the server locally (e.g. on port 8080):

```bash
locust -f locustfile_local.py \
  --headless \
  --host http://localhost:8080 \
  --run-time 20s \
  --queries-file queries.json \
  --users 20 \
  --processes 10 \
  --spawn-rate 20
```

### install wrk
```bash
sudo yum update -y
sudo yum groupinstall -y "Development Tools"
sudo yum install -y git openssl-devel

git clone https://github.com/wg/wrk.git
cd wrk
make

# 安装到系统 PATH（推荐）
sudo cp wrk /usr/local/bin/
wrk -v
```