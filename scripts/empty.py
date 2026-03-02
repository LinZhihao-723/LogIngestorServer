#!/usr/bin/env python3
"""
Async SQS queue emptying using aiobotocore + asyncio.

Install:
  pip install aiobotocore

Examples:
  python empty_sqs_queues_async.py --workers "0-9,10-12" --region us-east-2 --concurrency 8 --progress

Credential handling:
- By default uses standard AWS credential resolution (env vars, ~/.aws, IAM role).
- You can also pass explicit credentials flags (be cautious).
"""

import argparse
import asyncio
import sys
from typing import List, Optional, Tuple

from aiobotocore.session import get_session

ACCOUNT_ID = "568954113123"
REGION_DEFAULT = "us-east-2"


def worker_name_from_id(worker_id: int) -> str:
    if worker_id < 10:
        return f"LIB{worker_id}"
    letter_suffix = chr(ord("a") + (worker_id - 10))
    return f"LIB{letter_suffix}"


def queue_url_from_worker_id(worker_id: int, region: str) -> str:
    worker_name = worker_name_from_id(worker_id)
    return f"https://sqs.{region}.amazonaws.com/{ACCOUNT_ID}/{worker_name}"


def parse_worker_ids(spec: str) -> List[int]:
    ids = set()
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    for part in parts:
        if "-" in part:
            a, b = part.split("-", 1)
            start = int(a.strip())
            end = int(b.strip())
            if end < start:
                raise ValueError(f"Invalid range '{part}' (end < start)")
            for i in range(start, end + 1):
                ids.add(i)
        else:
            ids.add(int(part))
    return sorted(ids)


async def receive_and_delete_batch(
    sqs,
    queue_url: str,
    wait_time: int,
    visibility_timeout: int,
) -> int:
    resp = await sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=wait_time,
        VisibilityTimeout=visibility_timeout,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    )

    messages = resp.get("Messages", [])
    if not messages:
        return 0

    entries = [{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(messages)]
    del_resp = await sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)

    failed = del_resp.get("Failed", [])
    if failed:
        raise RuntimeError(f"delete_message_batch failures: {failed}")

    return len(messages)


async def empty_one_queue(
    sqs,
    queue_url: str,
    *,
    wait_time_seconds: int,
    visibility_timeout: int,
    consecutive_empty_polls: int,
    progress: bool,
) -> int:
    total_deleted = 0
    empty_polls = 0

    while True:
        deleted = await receive_and_delete_batch(
            sqs,
            queue_url,
            wait_time=wait_time_seconds,
            visibility_timeout=visibility_timeout,
        )

        if deleted == 0:
            empty_polls += 1
            if empty_polls >= consecutive_empty_polls:
                break
            # avoid hot loop if wait_time_seconds=0
            await asyncio.sleep(0.05)
            continue

        empty_polls = 0
        total_deleted += deleted
        if progress:
            print(f"[{queue_url}] +{deleted} (total {total_deleted})", flush=True)

    return total_deleted


async def worker_task(
    sem: asyncio.Semaphore,
    sqs,
    worker_id: int,
    args,
) -> Tuple[int, str, int]:
    queue_url = queue_url_from_worker_id(worker_id, args.region)
    name = worker_name_from_id(worker_id)

    async with sem:
        if args.progress:
            print(f"Start worker_id={worker_id} queue={queue_url}", flush=True)

        deleted = await empty_one_queue(
            sqs,
            queue_url,
            wait_time_seconds=args.wait_time_seconds,
            visibility_timeout=args.visibility_timeout,
            consecutive_empty_polls=args.consecutive_empty_polls,
            progress=args.progress,
        )

        return worker_id, name, deleted


async def run_async(args) -> int:
    worker_ids = parse_worker_ids(args.workers)
    if worker_ids and max(worker_ids) >= 36:
        print(
            "Error: worker_id >= 36 would map past 'z' (10->a ... 35->z). Adjust mapping if needed.",
            file=sys.stderr,
        )
        return 2

    session = get_session()
    client_kwargs = {"region_name": args.region}

    # Optional explicit credentials (otherwise standard AWS resolution)
    if args.access_key_id and args.secret_access_key:
        client_kwargs.update(
            aws_access_key_id=args.access_key_id,
            aws_secret_access_key=args.secret_access_key,
            aws_session_token=args.session_token,
        )

    sem = asyncio.Semaphore(args.concurrency)

    async with session.create_client("sqs", **client_kwargs) as sqs:
        tasks = [asyncio.create_task(worker_task(sem, sqs, wid, args)) for wid in worker_ids]
        results = await asyncio.gather(*tasks)

    grand_total = 0
    for wid, name, deleted in results:
        grand_total += deleted
        print(f"[{wid} {name}] Deleted {deleted} messages.")

    print(f"Done. Deleted {grand_total} messages across {len(worker_ids)} queue(s).")
    return 0


def main(argv) -> int:
    p = argparse.ArgumentParser(description="Async empty multiple SQS queues for LIB workers.")
    p.add_argument("--region", default=REGION_DEFAULT, help=f"AWS region. Default: {REGION_DEFAULT}")
    p.add_argument("--workers", required=True, help='Worker id list/range, e.g. "0-9,10-12".')

    p.add_argument("--concurrency", type=int, default=8, help="How many queues to drain concurrently. Default: 8.")

    p.add_argument("--wait-time-seconds", type=int, default=10, help="Long poll wait time (0-20). Default: 10.")
    p.add_argument("--visibility-timeout", type=int, default=30, help="Visibility timeout for received messages.")
    p.add_argument(
        "--consecutive-empty-polls",
        type=int,
        default=3,
        help="Stop after this many consecutive empty receives per queue. Default: 3.",
    )
    p.add_argument("--progress", action="store_true", help="Print progress while deleting.")

    # Optional explicit credentials
    p.add_argument("--access-key-id", help="AWS access key id (optional).")
    p.add_argument("--secret-access-key", help="AWS secret access key (optional).")
    p.add_argument("--session-token", help="AWS session token (optional).")

    args = p.parse_args(argv)

    if (args.access_key_id and not args.secret_access_key) or (args.secret_access_key and not args.access_key_id):
        print(
            "Error: if providing explicit credentials, you must pass both --access-key-id and --secret-access-key.",
            file=sys.stderr,
        )
        return 2

    return asyncio.run(run_async(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
