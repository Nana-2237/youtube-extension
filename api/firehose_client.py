import json
import time
import boto3
from botocore.exceptions import ClientError
from config import (
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    FIREHOSE_STREAM_NAME,
    BATCH_MAX_RECORDS,
    BATCH_FLUSH_MS
)

firehose_kwargs = {"region_name": AWS_REGION}
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    firehose_kwargs.update({
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY
    })
firehose = boto3.client("firehose", **firehose_kwargs)

_batch = []
_last_flush = 0.0


def send_batch(events: list[dict]) -> None:
    records = [{"Data": (json.dumps(ev, ensure_ascii=False) + "\n").encode("utf-8")} for ev in events]
    resp = firehose.put_record_batch(
        DeliveryStreamName=FIREHOSE_STREAM_NAME,
        Records=records
    )
    failed = resp.get("FailedPutCount", 0)
    if failed:
        raise RuntimeError(f"Firehose batch failed: {failed} records failed. resp={resp}")


def flush(force: bool = False) -> None:
    global _batch, _last_flush
    if not _batch:
        return
    now = time.time()
    if not force:
        if len(_batch) < BATCH_MAX_RECORDS and (now - _last_flush) * 1000 < BATCH_FLUSH_MS:
            return
    events = _batch
    _batch = []
    _last_flush = now
    try:
        send_batch(events)
    except (ClientError, RuntimeError) as e:
        _batch = events + _batch
        raise
    print(f"Firehose batch OK: sent={len(events)}")


def add_event(event: dict) -> None:
    global _batch
    _batch.append(event)

