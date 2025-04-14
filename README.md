# opensearch-semantic-benchmarks-sample

## Prepare the environment
1. Install conda, activate virtual env
```
wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh
bash Anaconda3-2023.03-1-Linux-x86_64.sh
conda create -n benchmark python=3.9
conda activate benchmark
```
2. Install dependencies
```
pip install opensearch-benchmark=1.11.0 beir ipykernel
```