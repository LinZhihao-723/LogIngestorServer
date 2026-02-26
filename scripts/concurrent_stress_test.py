#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
import time


async def run_upload_worker(worker_id, args):
    """Run a single upload worker subprocess."""
    # First 10 workers: LIB0-LIB9, next 10 workers: LIBa-LIBj
    if worker_id < 10:
        worker_name = f"LIB{worker_id}"
    else:
        letter_suffix = chr(ord('a') + (worker_id - 10))
        worker_name = f"LIB{letter_suffix}"

    queue_url = f"https://sqs.us-east-2.amazonaws.com/568954113123/{worker_name}"
    extra_config = json.dumps({"queue_url": queue_url, "num_concurrent_listener_tasks": 16})

    cmd = [
        sys.executable,  # Use the same Python interpreter
        "stress_upload.py",
        "--s3-endpoint", args.endpoint,
        "--s3-key", args.key,
        "--s3-secret", args.secret,
        "--s3-bucket", args.bucket,
        "--prefix", worker_name,
        "--num-files", str(args.num_files),
        "--submission-endpoint", "http://localhost:3002/sqs_listener",
        "--extra-submission-config", extra_config,
    ]

    if args.region:
        cmd.extend(["--s3-region", args.region])

    print(f"[Worker {worker_name}] Starting with queue: {queue_url}")

    start_time = time.time()

    # Create subprocess
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Read output in real-time
    async def read_stream(stream, prefix):
        while True:
            line = await stream.readline()
            if not line:
                break
            print(f"[Worker {worker_name}] {prefix}: {line.decode().rstrip()}")

    # Run both stdout and stderr readers concurrently
    await asyncio.gather(
        read_stream(process.stdout, "OUT"),
        read_stream(process.stderr, "ERR")
    )

    # Wait for process to complete
    returncode = await process.wait()

    elapsed_time = time.time() - start_time

    if returncode == 0:
        print(f"[Worker {worker_name}] ✓ Completed successfully in {elapsed_time:.2f}s")
    else:
        print(f"[Worker {worker_name}] ✗ Failed with return code {returncode}")

    return {
        'worker_id': worker_name,
        'returncode': returncode,
        'elapsed_time': elapsed_time,
        'num_files': args.num_files if returncode == 0 else 0
    }


def get_worker_list(num_workers):
    """Generate list of worker names based on num_workers."""
    workers = []
    for i in range(num_workers):
        if i < 10:
            workers.append(f"LIB{i}")
        else:
            letter_suffix = chr(ord('a') + (i - 10))
            workers.append(f"LIB{letter_suffix}")
    return workers


async def run_all_workers(args):
    """Run all workers concurrently."""
    num_workers = args.num_workers
    total_files = args.num_files * num_workers
    worker_list = get_worker_list(num_workers)

    print("=" * 80)
    print(f"Starting stress test with {num_workers} workers")
    print(f"  Workers: {', '.join(worker_list)}")
    print(f"  S3 Endpoint: {args.endpoint}")
    print(f"  S3 Bucket: {args.bucket}")
    print(f"  S3 Region: {args.region or 'us-east-1 (default)'}")
    print(f"  Files per worker: {args.num_files}")
    print(f"  Total files: {total_files}")
    print(f"  Submission endpoint: http://localhost:3002/sqs_listener")
    print("=" * 80)
    print()

    # Start overall timer
    overall_start_time = time.time()

    # Create tasks for all workers
    tasks = [run_upload_worker(i, args) for i in range(num_workers)]

    # Run all workers concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Calculate overall elapsed time
    overall_elapsed_time = time.time() - overall_start_time

    # Print summary
    print()
    print("=" * 80)
    print("Stress test completed!")
    print("=" * 80)

    success_count = 0
    failed_count = 0
    total_files_uploaded = 0
    total_worker_time = 0
    worker_details = []

    for result in results:
        if isinstance(result, Exception):
            print(f"Worker failed with exception: {result}")
            failed_count += 1
        else:
            worker_details.append(result)
            if result['returncode'] == 0:
                success_count += 1
                total_files_uploaded += result['num_files']
                total_worker_time += result['elapsed_time']
            else:
                failed_count += 1

    # Print per-worker details
    print(f"\n--- Per-Worker Results ---")
    for detail in worker_details:
        status = "✓" if detail['returncode'] == 0 else "✗"
        throughput = detail['num_files'] / detail['elapsed_time'] if detail[
                                                                         'elapsed_time'] > 0 else 0
        print(f"{status} {detail['worker_id']}: {detail['elapsed_time']:.2f}s, "
              f"{detail['num_files']} files, "
              f"{throughput:.2f} files/s")

    print(f"\n--- Summary ---")
    print(f"Successful workers: {success_count}/{num_workers}")
    print(f"Failed workers: {failed_count}/{num_workers}")
    print(f"Total files uploaded: {total_files_uploaded}/{total_files}")

    print(f"\n--- Throughput Metrics ---")
    print(f"Overall wall-clock time: {overall_elapsed_time:.2f}s")

    if total_files_uploaded > 0:
        # Overall throughput (wall-clock time)
        overall_throughput = total_files_uploaded / overall_elapsed_time
        print(f"Overall throughput: {overall_throughput:.2f} files/second")

        # Average per-worker throughput
        if success_count > 0:
            avg_worker_time = total_worker_time / success_count
            avg_worker_throughput = args.num_files / avg_worker_time
            print(f"Average per-worker time: {avg_worker_time:.2f}s")
            print(f"Average per-worker throughput: {avg_worker_throughput:.2f} files/second")

        # Aggregate throughput (sum of all worker throughputs)
        aggregate_throughput = total_files_uploaded / (
                total_worker_time / success_count) if success_count > 0 else 0
        print(f"Aggregate throughput (parallel): {aggregate_throughput:.2f} files/second")

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run stress test with configurable concurrent upload workers (LIB0-LIB9 and LIBa-LIBj)."
    )
    parser.add_argument("--endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument("--key", required=True, help="S3 credential key")
    parser.add_argument("--secret", required=True, help="S3 secret key")
    parser.add_argument("--region", required=False, default=None, help="S3 region")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--num-files",
        type=int,
        default=10,
        help="Number of files per worker (default: 10)"
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=10,
        help="Number of workers to run (1-20, default: 10). "
             "Workers 0-9 are LIB0-LIB9, workers 10-19 are LIBa-LIBj"
    )

    args = parser.parse_args()

    # Validate num_workers
    if args.num_workers < 1 or args.num_workers > 20:
        print("Error: --num-workers must be between 1 and 20")
        sys.exit(1)

    # Run the async event loop
    exit_code = asyncio.run(run_all_workers(args))
    sys.exit(exit_code)