import json
import traceback
from urllib.parse import unquote_plus
from s3_client import s3
from config import RAW_PREFIX, RESULTS_PREFIX, PROCESSED_PREFIX
from utils import day_partition_from_key_or_fallback, hash_key, decode_body
from aggregator import aggregate_ndjson
from s3_operations import write_json, move_raw_to_processed


def process_record(rec: dict) -> bool:
    try:
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])
    except Exception:
        print("Skipping record: not an S3 put event")
        return False

    if not key.startswith(RAW_PREFIX):
        print(f"Skipping key (not under {RAW_PREFIX}): {key}")
        return False

    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head.get("ETag", "").strip('"')
        print(f"Processing s3://{bucket}/{key} etag={etag}")

        obj = s3.get_object(Bucket=bucket, Key=key)
        body_stream = obj["Body"]
        try:
            body_bytes = body_stream.read()
        finally:
            body_stream.close()

        ndjson_text = decode_body(key, body_bytes)
        agg = aggregate_ndjson(ndjson_text)

        yyyy, mm, dd = day_partition_from_key_or_fallback(key)
        raw_hash = hash_key(key, etag)

        out_key = (
            f"{RESULTS_PREFIX}daily/{yyyy}/{mm}/{dd}/partials/"
            f"{raw_hash}.json"
        )

        payload = {
            "source": {
                "bucket": bucket,
                "key": key,
                "etag": etag,
            },
            "day_partition": {"yyyy": yyyy, "mm": mm, "dd": dd},
            **agg,
        }

        write_json(bucket, out_key, payload)
        print(f"Wrote partial results to s3://{bucket}/{out_key}")

        processed_key = key.replace(RAW_PREFIX, PROCESSED_PREFIX, 1)
        move_raw_to_processed(bucket, key, processed_key)
        print(f"Moved raw -> processed: s3://{bucket}/{processed_key}")

        return True
    except Exception as e:
        print(f"Error processing s3://{bucket}/{key}: {e}")
        print(traceback.format_exc())
        return False


def lambda_handler(event, context):
    print("Event:", json.dumps(event))
    records = event.get("Records", [])
    if not records:
        print("No Records found in event.")
        return {"ok": True, "processed": 0}

    processed_count = sum(1 for rec in records if process_record(rec))
    return {"ok": True, "processed": processed_count}
