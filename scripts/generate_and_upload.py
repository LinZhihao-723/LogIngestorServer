from clp_ffi_py.ir import Serializer
from clp_ffi_py.utils import serialize_dict_to_msgpack
import zstandard
from datetime import datetime, timezone
import random
import time
import argparse
import boto3

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

def generate_random(n=1000, sleep_time=0.005):
    cctx = zstandard.ZstdCompressor(level=3)
    with open("temp.clp.zstd", "wb") as raw_stream, cctx.stream_writer(raw_stream) as compressor:
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
    parser.add_argument("--s3-secret", required=True, help="S3 secret key")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--num-files", type=int, default=1, help="Number of files to generate")
    args = parser.parse_args()

    prefix = f"test-{round(time.time())}"
    print(f"Uploading to prefix: {prefix}")
    region = "us-east-2"
    s3_client = boto3.client(
        's3',
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_key,
        aws_secret_access_key=args.s3_secret,
        region_name=region
    )
    for idx in range(args.num_files):
        filename = f"log_{round(time.time())}.clp.zstd"
        generate_random()
        s3_key = f"{prefix}/{filename}"
        s3_client.upload_file("temp.clp.zstd", args.s3_bucket, s3_key)
        print(f"Uploaded temp.clp.zstd to s3://{args.s3_bucket}/{s3_key}")
