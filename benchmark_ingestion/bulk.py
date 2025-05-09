import json
import boto3
import argparse
import time
import os

from tqdm import tqdm
from opensearchpy import OpenSearch
from requests_aws4auth import AWS4Auth


def get_aws_auth(service="aoss"):
    credentials = boto3.Session().get_credentials()
    aws_auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        args.region,
        service,
        session_token=credentials.token,
    )
    return aws_auth


def retry(bulk_body, r):
    if r["errors"] == False:
        return
    with open("error.json", "w") as f:
        json.dump(r, f)
    failed = [
        idx for idx in range(len(r["items"])) if "error" in r["items"][idx]["index"]
    ]
    new_bulk_body = []
    for idx in failed:
        new_bulk_body = new_bulk_body + bulk_body[idx * 2 : idx * 2 + 2]
    print(args.rank, len(bulk_body), len(new_bulk_body))
    time.sleep(3)
    new_r = client.bulk(new_bulk_body, timeout=1000)
    retry(new_bulk_body, new_r)


def read_line_by_index(jsonl_file, offsets, line_index):
    if line_index < 0 or line_index >= len(offsets):
        raise ValueError(f"Invalid line index: {line_index}")

    with open(jsonl_file, "rb") as f:
        f.seek(offsets[line_index])
        line = json.loads(f.readline().decode("utf-8"))
        return line


parser = argparse.ArgumentParser()
parser.add_argument("--rank", help="display a square of a given number", type=int)
parser.add_argument("--total", help="display a square of a given number", type=int)
parser.add_argument("--index_name", type=str, required=True)
parser.add_argument("--file_name", type=str, required=True)
args = parser.parse_args()
print(args)

step = 400
index_name = args.index_name
jsonl_file = f"{args.file_name}.jsonl"
offset_file = f"{args.file_name}.offset"


# client = OpenSearch(
#     hosts=[{'host': host, 'port': port}],
#     http_auth=get_aws_auth(),
#     use_ssl=True,
#     verify_certs=True,
#     connection_class=RequestsHttpConnection
# )
client = OpenSearch(hosts=os.environ["HOSTS"])

with open(offset_file, "r") as f:
    offsets = [int(line.strip()) for line in f]

all_idxs = [i for i in range(len(offsets)) if i % args.total == args.rank]

for i in tqdm(range(0, len(all_idxs), step)):
    bulk_body = []
    idxs = all_idxs[i : min(i + step, len(all_idxs))]
    for idx in idxs:
        line = read_line_by_index(jsonl_file, offsets, idx)
        bulk_body.append({"index": {"_index": index_name}})
        bulk_body.append(line)
    r = client.bulk(bulk_body, timeout=1000)
    retry(bulk_body, r)
