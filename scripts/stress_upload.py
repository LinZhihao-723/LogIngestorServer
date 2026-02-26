from datetime import datetime, timezone
import time
import argparse
import boto3
from botocore.config import Config
import json
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from functools import partial

prefix = f"test-{round(time.time() * 1000 * 1000)}"


def read_file_once(filepath):
    """Read file into memory once to avoid repeated disk I/O."""
    with open(filepath, 'rb') as f:
        return f.read()


def upload_file_from_memory(s3_client, bucket_name, file_key, file_data):
    """Upload file data directly from memory using put_object."""
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=file_data
    )
    return file_key


async def upload_file_async(s3_client, bucket_name, file_id, file_data, executor):
    """Upload a file to S3 asynchronously using a thread pool."""
    loop = asyncio.get_event_loop()
    file_key = f"{prefix}/{file_id:05d}.clp.zstd"

    # Use put_object with in-memory data instead of upload_file
    await loop.run_in_executor(
        executor,
        upload_file_from_memory,
        s3_client,
        bucket_name,
        file_key,
        file_data
    )

    print(f"Uploaded sample.clp.zstd to s3://{bucket_name}/{file_key}")
    return file_key


async def upload_batch_worker(s3_client, bucket_name, file_queue, file_data, executor,
                              batch_size=2):
    """Worker that processes multiple uploads concurrently."""
    while True:
        # Get a batch of file IDs
        batch = []
        for _ in range(batch_size):
            try:
                file_id = await asyncio.wait_for(file_queue.get(), timeout=0.1)
                if file_id is None:  # Sentinel value
                    file_queue.task_done()
                    # Put sentinel back for other workers
                    if len(batch) == 0:
                        return
                    break
                batch.append(file_id)
            except asyncio.TimeoutError:
                break

        if not batch:
            continue

        # Upload batch concurrently
        try:
            upload_tasks = [
                upload_file_async(s3_client, bucket_name, file_id, file_data, executor)
                for file_id in batch
            ]
            await asyncio.gather(*upload_tasks)
        except Exception as e:
            print(f"Failed to upload batch: {e}")
        finally:
            for _ in batch:
                file_queue.task_done()


async def upload_files_concurrent(s3_client, bucket_name, num_files, file_data, num_workers=10,
                                  batch_size=2):
    """Upload files concurrently using multiple worker coroutines with batching."""
    file_queue = asyncio.Queue()

    # Create thread pool executor - increase max_workers for better parallelism
    executor = ThreadPoolExecutor(max_workers=num_workers * batch_size)

    # Create worker tasks
    workers = [
        asyncio.create_task(
            upload_batch_worker(s3_client, bucket_name, file_queue, file_data, executor, batch_size)
        )
        for _ in range(num_workers)
    ]

    # Enqueue all file IDs
    for file_id in range(1, num_files + 1):
        await file_queue.put(file_id)

    # Wait for all uploads to complete
    await file_queue.join()

    # Stop workers
    for _ in range(num_workers):
        await file_queue.put(None)

    # Wait for workers to finish
    await asyncio.gather(*workers)

    # Shutdown executor
    executor.shutdown(wait=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and upload log files to S3.")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument("--s3-key", required=True, help="S3 credential key")
    parser.add_argument("--s3-region", required=False, default=None, help="S3 region")
    parser.add_argument("--s3-secret", required=True, help="S3 secret key")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--num-files", type=int, default=1, help="Number of files to generate")
    parser.add_argument(
        "--submission-endpoint",
        required=True,
        help="Ingestion job submission endpoint"
    )
    parser.add_argument(
        "--extra-submission-config",
        required=False,
        help="Ingestion job config (no need for key-prefix and bucket)"
    )
    parser.add_argument(
        "--prefix",
        required=False,
        help="S3 key prefix (added to the generated prefix)"
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=16,
        help="Number of worker coroutines"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=4,
        help="Number of concurrent uploads per worker"
    )
    args = parser.parse_args()

    if args.prefix:
        prefix = f"{args.prefix}/{prefix}"

    # Check if sample file exists
    if not os.path.exists("sample.clp.zstd"):
        print("Error: sample.clp.zstd not found in current directory")
        exit(1)

    # Read file into memory once
    print("Reading sample file into memory...")
    file_data = read_file_once("sample.clp.zstd")
    file_size_mb = len(file_data) / (1024 * 1024)
    print(f"File size: {file_size_mb:.2f} MB")

    clp_s3_endpoint = args.s3_endpoint
    if clp_s3_endpoint.startswith("http://localhost:"):
        clp_s3_endpoint = "http://host.docker.internal:" + clp_s3_endpoint.split("localhost:")[1]
    elif clp_s3_endpoint.endswith("amazonaws.com"):
        clp_s3_endpoint = None

    ingestion_job_config = {
        "endpoint_url": clp_s3_endpoint,
        "bucket_name": args.s3_bucket,
        "key_prefix": prefix,
        "dataset": args.prefix,
    }

    if args.extra_submission_config:
        extra_config = json.loads(args.extra_submission_config)
        ingestion_job_config |= extra_config

    if args.s3_region:
        ingestion_job_config |= {"region": args.s3_region}

    print(f"Ingestion job config: {json.dumps(ingestion_job_config)}")

    try:
        resp = requests.post(args.submission_endpoint, json=ingestion_job_config, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Submission failed: {e}")
        exit(1)
    print(f"Submission succeeded: {resp.status_code} - {resp.text}")

    print(f"Uploading to prefix: {prefix}")
    region = args.s3_region if args.s3_region else "us-east-1"

    # Configure boto3 for high concurrency
    boto_config = Config(
        max_pool_connections=args.num_workers * args.batch_size,
        retries={'max_attempts': 3, 'mode': 'adaptive'}
    )

    s3_client = boto3.client(
        's3',
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_key,
        aws_secret_access_key=args.s3_secret,
        region_name=region,
        config=boto_config
    )

    # Run async upload with configurable workers and batch size
    start_time = time.time()
    asyncio.run(
        upload_files_concurrent(
            s3_client,
            args.s3_bucket,
            args.num_files,
            file_data,
            num_workers=args.num_workers,
            batch_size=args.batch_size
        )
    )
    elapsed_time = time.time() - start_time

    total_data_mb = file_size_mb * args.num_files
    print(f"\nCompleted uploading {args.num_files} files in {elapsed_time:.2f} seconds")
    print(f"Average throughput: {args.num_files / elapsed_time:.2f} files/second")
    print(f"Data throughput: {total_data_mb / elapsed_time:.2f} MB/second")
