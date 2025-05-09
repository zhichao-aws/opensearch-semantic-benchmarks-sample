import subprocess
import signal
import sys
import time
import os
import argparse

from tqdm import tqdm


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

    # 启动子进程
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
        ]

        # 创建子进程
        process = subprocess.Popen(cmd)
        processes.append(process)
        print(f"Started process for rank {rank} with PID {process.pid}")

    # 处理信号，确保清理子进程
    def signal_handler(signum, frame):
        print("\nReceived signal to terminate. Cleaning up...")
        cleanup_processes(processes)
        sys.exit(0)

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 终止信号

    try:
        # 等待所有进程完成
        while True:
            all_done = True
            for i, proc in enumerate(processes):
                if proc.poll() is None:  # 进程还在运行
                    all_done = False
                else:
                    # 进程已结束，检查返回码
                    return_code = proc.poll()
                    if return_code != 0:
                        print(f"Process {i} failed with return code {return_code}")
                        # 获取错误输出
                        _, stderr = proc.communicate()
                        print(f"Error output: {stderr}")

            if all_done:
                print("All processes completed successfully")
                break

            time.sleep(1)  # 避免过度消耗CPU

    except Exception as e:
        print(f"Error occurred: {e}")
        cleanup_processes(processes)
        sys.exit(1)

    finally:
        cleanup_processes(processes)


def cleanup_processes(processes):
    """清理所有子进程"""
    for proc in processes:
        try:
            if proc.poll() is None:  # 如果进程还在运行
                # 在 Windows 上使用 taskkill
                if os.name == "nt":
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)])
                # 在 Unix-like 系统上使用 SIGTERM
                else:
                    proc.terminate()  # 发送 SIGTERM
                    # 给进程一些时间来清理
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()  # 如果进程没有及时响应，强制结束

                print(f"Terminated process {proc.pid}")
        except Exception as e:
            print(f"Error while terminating process {proc.pid}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total_ranks", help="process number", type=int, default=8)
    parser.add_argument("--index_name", type=str, required=True)
    parser.add_argument("--file_name", type=str, required=True)

    args = parser.parse_args()
    print(args)

    jsonl_file = f"{args.file_name}.jsonl"
    offset_file = f"{args.file_name}.offset"
    if not os.path.exists(offset_file):
        total_lines = create_offset_file(jsonl_file, offset_file)
        print(f"Created offset file. Total lines: {total_lines}")

    run_processes(args)
