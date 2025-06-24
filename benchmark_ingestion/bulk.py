import json
import argparse
import time

from tqdm import tqdm
from utils import get_os_client
from dotenv import load_dotenv

load_dotenv()


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
    print(
        f"Failed bulk. Process rank:{args.rank}: {len(bulk_body)//2} -> {len(new_bulk_body)//2}"
    )
    time.sleep(1)
    new_r = client.bulk(new_bulk_body)
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
parser.add_argument(
    "--use_aws_auth", action="store_true", help="whether to use aws auth"
)
parser.add_argument("--bulk_size", type=int, default=10, help="bulk size")
parser.add_argument("--region", type=str, default="us-east-1", help="AWS region")
args = parser.parse_args()
print(args)

bulk_size = args.bulk_size
index_name = args.index_name
jsonl_file = f"{args.file_name}.jsonl"
offset_file = f"{args.file_name}.offset"

client = get_os_client(use_aws_auth=args.use_aws_auth, region=args.region)

with open(offset_file, "r") as f:
    offsets = [int(line.strip()) for line in f]

all_idxs = [i for i in range(len(offsets)) if i % args.total == args.rank]

for i in tqdm(range(0, len(all_idxs), bulk_size)):
    bulk_body = []
    idxs = all_idxs[i : min(i + bulk_size, len(all_idxs))]
    for idx in idxs:
        line = read_line_by_index(jsonl_file, offsets, idx)
        bulk_body.append({"index": {"_index": index_name}})
        bulk_body.append(line)
    r = client.bulk(bulk_body)
    retry(bulk_body, r)
