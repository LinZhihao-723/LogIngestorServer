#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
import time


async def run_upload_worker(worker_id, args):
    """Run a single upload worker subprocess."""
    queue_url = f"https://sqs.us-east-2.amazonaws.com/568954113123/LIB{worker_id}"
    extra_config = json.dumps({"queue_url": queue_url})

    cmd = [
        sys.executable,  # Use the same Python interpreter
        "stress_upload.py",
        "--s3-endpoint", args.endpoint,
        "--s3-key", args.key,
        "--s3-secret", args.secret,
        "--s3-bucket", args.bucket,
        "--prefix", f"LIB{worker_id}",
        "--num-files", str(args.num_files),
        "--submission-endpoint", "http://localhost:3002/sqs_listener",
        "--extra-submission-config", extra_config,
    ]

    if args.region:
        cmd.extend(["--s3-region", args.region])

    print(f"[Worker {worker_id}] Starting with queue: {queue_url}")

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
            print(f"[Worker {worker_id}] {prefix}: {line.decode().rstrip()}")

    # Run both stdout and stderr readers concurrently
    await asyncio.gather(
        read_stream(process.stdout, "OUT"),
        read_stream(process.stderr, "ERR")
    )

    # Wait for process to complete
    returncode = await process.wait()

    elapsed_time = time.time() - start_time

    if returncode == 0:
        print(f"[Worker {worker_id}] ✓ Completed successfully in {elapsed_time:.2f}s")
    else:
        print(f"[Worker {worker_id}] ✗ Failed with return code {returncode}")

    return {
        'worker_id': worker_id,
        'returncode': returncode,
        'elapsed_time': elapsed_time,
        'num_files': args.num_files if returncode == 0 else 0
    }


async def run_all_workers(args):
    """Run all 10 workers concurrently."""
    print("=" * 80)
    print(f"Starting stress test with 10 workers")
    print(f"  S3 Endpoint: {args.endpoint}")
    print(f"  S3 Bucket: {args.bucket}")
    print(f"  S3 Region: {args.region or 'us-east-1 (default)'}")
    print(f"  Files per worker: {args.num_files}")
    print(f"  Total files: {args.num_files * 10}")
    print(f"  Submission endpoint: http://localhost:3002/sqs_listener")
    print("=" * 80)
    print()

    # Start overall timer
    overall_start_time = time.time()

    # Create tasks for all 10 workers
    tasks = [run_upload_worker(i, args) for i in range(10)]

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

    for result in results:
        if isinstance(result, Exception):
            print(f"Worker failed with exception: {result}")
            failed_count += 1
        else:
            if result['returncode'] == 0:
                success_count += 1
                total_files_uploaded += result['num_files']
                total_worker_time += result['elapsed_time']
            else:
                failed_count += 1

    print(f"\nSuccessful workers: {success_count}/10")
    print(f"Failed workers: {failed_count}/10")
    print(f"Total files uploaded: {total_files_uploaded}")
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
        description="Run stress test with 10 concurrent upload workers."
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

    args = parser.parse_args()

    # Run the async event loop
    exit_code = asyncio.run(run_all_workers(args))
    sys.exit(exit_code)
