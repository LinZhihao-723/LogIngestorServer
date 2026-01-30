from clp_ffi_py.ir import Serializer
from clp_ffi_py.utils import serialize_dict_to_msgpack
import zstandard
from datetime import datetime, timezone
import random
import time
import argparse
import boto3
import json
import requests

messages = [
    "Service started.",
    "Connection established.",
    "User logged in.",
    "Error: Invalid credentials.",
    "Data processed successfully.",
    "Timeout occurred.",
    "Resource not found.",
    "Operation completed.",
    "Warning: Disk space low.",
    "Backup finished."
]

prefix = f"test-{round(time.time())}"

def generate_random(n=10, sleep_time=0.005):
    cctx = zstandard.ZstdCompressor(level=3)
    with open(f"{prefix}.clp.zstd", "wb") as raw_stream, cctx.stream_writer(raw_stream) as compressor:
        with Serializer(compressor) as serializer:
            for _ in range(n):
                serializer.serialize_log_event_from_msgpack_map(
                    auto_gen_msgpack_map=serialize_dict_to_msgpack({
                        "level": "INFO",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }),
                    user_gen_msgpack_map=serialize_dict_to_msgpack({
                        "message": random.choice(messages)
                    }),
                )
                time.sleep(sleep_time)


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
    args = parser.parse_args()

    global prefix
    if args.prefix:
        prefix = f"{args.prefix}/{prefix}"

    clp_s3_endpoint = args.s3_endpoint
    if clp_s3_endpoint.startswith("http://localhost:"):
        clp_s3_endpoint = "http://host.docker.internal:" + clp_s3_endpoint.split("localhost:")[1]
    elif clp_s3_endpoint.endswith("amazonaws.com"):
        clp_s3_endpoint = None

    ingestion_job_config = {
        "endpoint_url": clp_s3_endpoint,
        "bucket_name": args.s3_bucket,
        "key_prefix": prefix,
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
    s3_client = boto3.client(
        's3',
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_key,
        aws_secret_access_key=args.s3_secret,
        region_name=args.s3_region
    )
    for idx in range(args.num_files):
        filename = f"log_{round(time.time())}.clp.zstd"
        generate_random()
        s3_key = f"{prefix}/{filename}"
        s3_client.upload_file(f"{prefix}.clp.zstd", args.s3_bucket, s3_key)
        print(f"Uploaded {prefix}.clp.zstd to s3://{args.s3_bucket}/{s3_key}")
