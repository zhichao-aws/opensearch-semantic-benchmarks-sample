#!/usr/bin/env python3
"""
Automated performance benchmark script for SageMaker endpoints.

Usage:
python automated_benchmark.py --endpoint-name your-endpoint --params "[(1,2),(2,4),(4,8)]"
"""

import argparse
import json
import os
import subprocess
import time
import csv
from pathlib import Path
import numpy as np
import ast
from datetime import datetime


def parse_metrics_file(metrics_file):
    """
    Parse the metrics JSON file and return RPS as well as P90 latency.
    """
    try:
        with open(metrics_file, "r") as f:
            data = json.load(f)

        if not data or len(data) == 0:
            return None, None

        metrics = data[0]  # take the first element

        # Calculate RPS (exclude first 5 seconds, then sum all values divided by run_time)
        rps_per_sec = metrics.get("num_reqs_per_sec", {})
        if not rps_per_sec:
            return None, None

        # Filter out data points within 5 seconds of start time
        for key, value in list(rps_per_sec.items()):
            if float(key) - metrics.get("start_time", 0) <= 5:
                del rps_per_sec[key]
            else:
                rps_per_sec[key] = int(value)

        rps_values = list(rps_per_sec.values())
        run_time = metrics.get("last_request_timestamp") - metrics.get("start_time")
        rps = sum(rps_values) / run_time

        # Calculate P90 latency
        response_times = metrics.get("response_times", {})
        if not response_times:
            return rps, None

        # Expand response-time histogram into raw samples
        all_response_times = []
        for time_ms, count in response_times.items():
            all_response_times.extend([float(time_ms)] * count)

        if not all_response_times:
            return rps, None

        p90_latency = np.percentile(all_response_times, 90)

        return rps, p90_latency

    except Exception as e:
        print(f"Error parsing metrics file: {e}")
        return None, None


def run_locust_test(endpoint_name, size_per_doc, request_size, users, run_time=30):
    """
    Run a single locust test.
    """
    docs_per_request = request_size // size_per_doc

    # Generate a unique metrics file name
    timestamp = int(time.time())
    metrics_file = (
        f"metrics/test_{size_per_doc}kb_{docs_per_request}docs_{users}users_{timestamp}"
    )

    # Build the locust command and set environment variables
    os.environ["MAX_USERS"] = str(users)
    os.environ["SECOND_STAGE_DURATION"] = str(run_time)

    cmd = [
        "locust",
        "-f",
        "locust_benchmark_sm.py",
        "--headless",
        "--size-per-doc",
        str(size_per_doc),
        "--request-size",
        str(request_size),
        "--endpoint-name",
        endpoint_name,
        "--json-file",
        metrics_file,
    ]

    print(
        f"Running test: {size_per_doc}KB/doc, {docs_per_request} docs/request, {users} users"
    )
    print(f"Command: {' '.join(cmd)}")

    start_utc = datetime.utcnow().isoformat() + "Z"

    try:
        # Execute the locust command
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")

        if result.returncode != 0:
            print(f"Locust test failed: {result.stderr}")
            return None, None, start_utc, datetime.utcnow().isoformat() + "Z"

        # Wait a bit to ensure the file is flushed to disk
        time.sleep(1)

        # Parse test results
        full_metrics_path = Path(metrics_file + ".json")
        if full_metrics_path.exists():
            rps, p90_latency = parse_metrics_file(str(full_metrics_path))
            end_utc = datetime.utcnow().isoformat() + "Z"
            print(f"Result: RPS={rps:.2f}, P90 latency={p90_latency:.2f}ms")
            return rps, p90_latency, start_utc, end_utc
        else:
            print(f"Metrics file does not exist: {full_metrics_path}")
            return None, None, start_utc, datetime.utcnow().isoformat() + "Z"

    except Exception as e:
        print(f"Error running test: {e}")
        return None, None, start_utc, datetime.utcnow().isoformat() + "Z"


def test_parameter_set(
    endpoint_name,
    size_per_doc,
    docs_per_request,
    start_users=4,
    step_size=4,
    run_time=30,
):
    """
    Test a single parameter set, increasing the number of users until the RPS gain is below 10 %.
    """
    print(f"\nStarting parameter set test: ({size_per_doc}KB, {docs_per_request} docs)")

    # Determine starting users based on request size
    total_request_kb = size_per_doc * docs_per_request
    if total_request_kb < 10:
        current_users = max(start_users + 2 * step_size, 1)
        if current_users > start_users:
            print(
                f"Total request size {total_request_kb}KB < 10KB, starting users adjusted to {current_users} (start_users + 2*step_size)."
            )
    else:
        current_users = start_users

    results = []
    previous_rps = 0

    while True:
        request_size = size_per_doc * docs_per_request
        rps, p90_latency, start_utc, end_utc = run_locust_test(
            endpoint_name, size_per_doc, request_size, current_users, run_time
        )

        if rps is None or p90_latency is None:
            print(f"Test failed, end test with {current_users} users")
            break

        results.append(
            {
                "size_per_doc_kb": size_per_doc,
                "docs_per_request": docs_per_request,
                "users": current_users,
                "rps": rps,
                "p90_latency_ms": p90_latency,
                "start_time_utc": start_utc,
                "end_time_utc": end_utc,
            }
        )

        # Check whether we should continue to increase users
        if previous_rps > 0:
            improvement = (rps - previous_rps) / previous_rps
            print(f"RPS improvement: {improvement:.2%}")

            if improvement < 0.08:  # less than 8% improvement
                print("RPS improvement < 8 %, stopping user increase")
                break

        previous_rps = rps
        current_users += step_size

        # Safety limit: maximum number of users
        if current_users > 40:
            print("Reached maximum user limit (40), stopping test")
            break

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Automated SageMaker endpoint performance benchmark"
    )
    parser.add_argument(
        "--endpoint-name", required=True, help="Name of the SageMaker endpoint"
    )
    parser.add_argument(
        "--params",
        required=True,
        help='List of parameter tuples, e.g. "[(1,2),(2,4),(4,8)]" (size_per_doc_kb, docs_per_request)',
    )
    parser.add_argument(
        "--start-users",
        type=int,
        default=4,
        help="Starting number of users (default: 4)",
    )
    parser.add_argument(
        "--step-size", type=int, default=4, help="User increment step size (default: 4)"
    )
    parser.add_argument(
        "--run-time",
        type=int,
        default=30,
        help="Run time of each test in seconds (default: 30)",
    )
    parser.add_argument(
        "--output",
        default="benchmark_results.csv",
        help="Output CSV file name (default: benchmark_results.csv)",
    )

    args = parser.parse_args()

    # Parse parameter list
    try:
        param_sets = ast.literal_eval(args.params)
        if not isinstance(param_sets, list):
            raise ValueError("Parameters must be a list")
    except Exception as e:
        print(f"Parameter parsing error: {e}")
        print('Example format: "[(1,2),(2,4),(4,8)]"')
        return

    print("Starting performance tests...")
    print(f"Endpoint: {args.endpoint_name}")
    print(f"Parameter sets: {param_sets}")
    print(f"Start users: {args.start_users}")
    print(f"Step size: {args.step_size}")
    print(f"Run time per test: {args.run_time}s")

    # Ensure that the metrics directory exists
    metrics_dir = Path("metrics")
    metrics_dir.mkdir(exist_ok=True)

    # Run all tests
    all_results = []

    for size_per_doc, docs_per_request in param_sets:
        results = test_parameter_set(
            args.endpoint_name,
            size_per_doc,
            docs_per_request,
            args.start_users,
            args.step_size,
            args.run_time,
        )
        all_results.extend(results)

    # Save results to CSV
    if all_results:
        with open(args.output, "w", newline="") as csvfile:
            fieldnames = [
                "size_per_doc_kb",
                "docs_per_request",
                "users",
                "rps",
                "p90_latency_ms",
                "start_time_utc",
                "end_time_utc",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for result in all_results:
                writer.writerow(result)

        print(f"\nTests completed! Results saved to: {args.output}")

        # Print summary information
        print("\nSummary of test results:")
        print("Params | Best users | Max RPS | P90 latency")
        print("-" * 50)

        for size_per_doc, docs_per_request in param_sets:
            param_results = [
                r
                for r in all_results
                if r["size_per_doc_kb"] == size_per_doc
                and r["docs_per_request"] == docs_per_request
            ]
            if param_results:
                best_result = max(param_results, key=lambda x: x["rps"])
                print(
                    f"({size_per_doc}KB,{docs_per_request}docs) | {best_result['users']} | {best_result['rps']:.1f} | {best_result['p90_latency_ms']:.1f}ms"
                )
    else:
        print("No successful test results")


if __name__ == "__main__":
    main()
