import subprocess
import signal
import sys
import time
import os
import argparse
from dotenv import load_dotenv

from tqdm import tqdm

load_dotenv()


def create_offset_file(jsonl_file, offset_file):
    offsets = []
    with open(jsonl_file, "rb") as f:
        offset = 0
        for line in tqdm(f):
            offsets.append(offset)
            offset = f.tell()

    with open(offset_file, "w") as f:
        for offset in offsets:
            f.write(f"{offset}\n")

    return len(offsets)


def run_processes(args):
    processes = []

    # Start child processes
    for rank in range(args.total_ranks):
        cmd = [
            "python",
            "bulk.py",
            "--rank",
            str(rank),
            "--total",
            str(args.total_ranks),
            "--index_name",
            args.index_name,
            "--file_name",
            args.file_name,
            "--bulk_size",
            str(args.bulk_size),
            "--region",
            args.region,
        ]

        if args.use_aws_auth:
            cmd.append("--use_aws_auth")

        # Create child process
        process = subprocess.Popen(cmd)
        processes.append(process)
        print(f"Started process for rank {rank} with PID {process.pid}")

    # Handle signals to ensure cleanup of child processes
    def signal_handler(signum, frame):
        print("\nReceived signal to terminate. Cleaning up...")
        cleanup_processes(processes)
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Terminate signal

    try:
        # Wait for all processes to complete
        while True:
            all_done = True
            for i, proc in enumerate(processes):
                if proc.poll() is None:  # Process is still running
                    all_done = False
                else:
                    # Process has finished, check return code
                    return_code = proc.poll()
                    if return_code != 0:
                        print(f"Process {i} failed with return code {return_code}")
                        # Get error output
                        _, stderr = proc.communicate()
                        print(f"Error output: {stderr}")

            if all_done:
                print("All processes completed successfully")
                break

            time.sleep(1)  # Avoid excessive CPU consumption

    except Exception as e:
        print(f"Error occurred: {e}")
        cleanup_processes(processes)
        sys.exit(1)

    finally:
        cleanup_processes(processes)


def cleanup_processes(processes):
    """Clean up all child processes"""
    for proc in processes:
        try:
            if proc.poll() is None:  # If process is still running
                # Use taskkill on Windows
                if os.name == "nt":
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)])
                # Use SIGTERM on Unix-like systems
                else:
                    proc.terminate()  # Send SIGTERM
                    # Give process some time to clean up
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()  # Force kill if process doesn't respond in time

                print(f"Terminated process {proc.pid}")
        except Exception as e:
            print(f"Error while terminating process {proc.pid}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total_ranks", help="process number", type=int, default=8)
    parser.add_argument("--index_name", type=str, required=True)
    parser.add_argument("--file_name", type=str, required=True)
    parser.add_argument("--bulk_size", type=int, default=10, help="bulk size")
    parser.add_argument(
        "--use_aws_auth", action="store_true", help="whether to use aws auth"
    )
    parser.add_argument("--region", type=str, default="us-east-1", help="AWS region")

    args = parser.parse_args()
    print(args)

    jsonl_file = f"{args.file_name}.jsonl"
    offset_file = f"{args.file_name}.offset"
    if not os.path.exists(offset_file):
        total_lines = create_offset_file(jsonl_file, offset_file)
        print(f"Created offset file. Total lines: {total_lines}")

    run_processes(args)
