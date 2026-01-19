import boto3
import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional
from urllib.parse import unquote_plus
from botocore.config import Config

# Configure S3 client with appropriate settings for Lambda
s3_config = Config(
    max_pool_connections=50,
    retries={'max_attempts': 3, 'mode': 'standard'},
    connect_timeout=60,
    read_timeout=60
)
s3 = boto3.client("s3", config=s3_config)

# You can override these as Lambda environment variables if you want
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/")
RESULTS_PREFIX = os.getenv("RESULTS_PREFIX", "results/")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX", "raw-processed/")


def _safe_json_loads(line: str) -> Optional[Dict[str, Any]]:
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None


def _day_partition_from_key_or_fallback(key: str) -> Tuple[str, str, str]:
    """
    Prefer YYYY/MM/DD from the S3 key path: raw/YYYY/MM/DD/...
    Fallback: today's UTC date.
    """
    parts = key.split("/")
    # Expect: raw/YYYY/MM/DD/...
    if len(parts) >= 4 and parts[0] == RAW_PREFIX.rstrip("/"):
        yyyy, mm, dd = parts[1], parts[2], parts[3]
        if len(yyyy) == 4 and len(mm) == 2 and len(dd) == 2:
            return yyyy, mm, dd

    now = datetime.now(timezone.utc)
    return f"{now.year:04d}", f"{now.month:02d}", f"{now.day:02d}"


def _hash_key(key: str, etag: str = "") -> str:
    h = hashlib.sha256()
    h.update(key.encode("utf-8"))
    if etag:
        h.update(etag.encode("utf-8"))
    return h.hexdigest()[:16]


def _decode_body(key: str, body_bytes: bytes) -> str:
    # Firehose commonly delivers gzip when you enabled GZIP.
    if key.endswith(".gz"):
        return gzip.decompress(body_bytes).decode("utf-8", errors="replace")
    # Otherwise assume utf-8 text
    return body_bytes.decode("utf-8", errors="replace")


def _aggregate_ndjson(ndjson_text: str) -> Dict[str, Any]:
    """
    NEW schema aggregation:
    - Uses watch_tick.watch_ms_delta for time (not start/stop deltas)
    - Counts views via video_start
    - Supports foreground vs background totals via watch_mode
    """
    total_ms_by_channel: Dict[str, int] = {}
    total_ms_by_video: Dict[str, int] = {}
    total_ms_by_channel_fg: Dict[str, int] = {}
    total_ms_by_channel_bg: Dict[str, int] = {}

    views_by_video: Dict[str, int] = {}
    views_by_channel: Dict[str, int] = {}

    total_events = 0
    valid_events = 0
    invalid_events = 0

    ignored_no_video_ticks = 0
    ignored_no_channel_ticks = 0

    for raw_line in ndjson_text.splitlines():
        total_events += 1
        ev = _safe_json_loads(raw_line)
        if ev is None:
            invalid_events += 1
            continue

        # Minimal validation for new schema
        etype = ev.get("event_type")
        ts = ev.get("event_ts")
        tab_id = ev.get("tab_id")

        if etype is None or ts is None or tab_id is None:
            invalid_events += 1
            continue

        valid_events += 1

        # "Views" = count of video_start events
        if etype == "video_start":
            vid = ev.get("video_id")
            ch = ev.get("channel_name")
            if vid:
                views_by_video[vid] = views_by_video.get(vid, 0) + 1
            if ch:
                views_by_channel[ch] = views_by_channel.get(ch, 0) + 1
            continue

        # Watch time from watch_tick deltas
        if etype != "watch_tick":
            continue

        delta = ev.get("watch_ms_delta")
        if not isinstance(delta, int) or delta <= 0:
            continue

        video_id = ev.get("video_id")
        channel = ev.get("channel_name")
        watch_mode = ev.get("watch_mode")  # 'foreground' or 'background' (optional)

        if not video_id:
            ignored_no_video_ticks += 1
            continue

        # per video
        total_ms_by_video[video_id] = total_ms_by_video.get(video_id, 0) + delta

        # per channel
        if not channel:
            ignored_no_channel_ticks += 1
            continue

        total_ms_by_channel[channel] = total_ms_by_channel.get(channel, 0) + delta

        if watch_mode == "background":
            total_ms_by_channel_bg[channel] = total_ms_by_channel_bg.get(channel, 0) + delta
        else:
            total_ms_by_channel_fg[channel] = total_ms_by_channel_fg.get(channel, 0) + delta

    return {
        "totals": {
            "total_ms_by_channel": total_ms_by_channel,
            "total_ms_by_video": total_ms_by_video,
            "total_ms_by_channel_fg": total_ms_by_channel_fg,
            "total_ms_by_channel_bg": total_ms_by_channel_bg,
        },
        "views": {
            "views_by_video": views_by_video,
            "views_by_channel": views_by_channel,
        },
        "metrics": {
            "total_events": total_events,
            "valid_events": valid_events,
            "invalid_events": invalid_events,
            "ignored_no_video_ticks": ignored_no_video_ticks,
            "ignored_no_channel_ticks": ignored_no_channel_ticks,
        },
    }


def _write_json(bucket: str, key: str, obj: Dict[str, Any]) -> None:
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"Error writing to s3://{bucket}/{key}: {e}")
        raise


def _move_raw_to_processed(bucket: str, raw_key: str, processed_key: str) -> None:
    # Copy then delete
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": raw_key},
            Key=processed_key,
        )
        s3.delete_object(Bucket=bucket, Key=raw_key)
    except Exception as e:
        print(f"Error moving s3://{bucket}/{raw_key} to {processed_key}: {e}")
        raise


def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    # S3 trigger can contain multiple records
    records = event.get("Records", [])
    if not records:
        print("No Records found in event.")
        return {"ok": True, "processed": 0}

    processed_count = 0

    for rec in records:
        try:
            bucket = rec["s3"]["bucket"]["name"]
            # S3 notification keys can be URL-encoded
            key = unquote_plus(rec["s3"]["object"]["key"])
        except Exception:
            print("Skipping record: not an S3 put event")
            continue

        # Only handle raw/
        if not key.startswith(RAW_PREFIX):
            print(f"Skipping key (not under {RAW_PREFIX}): {key}")
            continue

        try:
            # Get the object (and ETag if present)
            head = s3.head_object(Bucket=bucket, Key=key)
            etag = head.get("ETag", "").strip('"')
            print(f"Processing s3://{bucket}/{key} etag={etag}")

            # Get object body - ensure stream is properly read
            obj = s3.get_object(Bucket=bucket, Key=key)
            body_stream = obj["Body"]
            try:
                body_bytes = body_stream.read()
            finally:
                # Ensure stream is closed
                body_stream.close()

            ndjson_text = _decode_body(key, body_bytes)
            agg = _aggregate_ndjson(ndjson_text)

            yyyy, mm, dd = _day_partition_from_key_or_fallback(key)
            raw_hash = _hash_key(key, etag)

            # Write partial results (safe for concurrency)
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

            _write_json(bucket, out_key, payload)
            print(f"Wrote partial results to s3://{bucket}/{out_key}")

            # Move raw -> raw-processed
            processed_key = key.replace(RAW_PREFIX, PROCESSED_PREFIX, 1)
            _move_raw_to_processed(bucket, key, processed_key)
            print(f"Moved raw -> processed: s3://{bucket}/{processed_key}")

            processed_count += 1
        except Exception as e:
            print(f"Error processing s3://{bucket}/{key}: {e}")
            # Log the full exception for debugging
            import traceback
            print(traceback.format_exc())
            # Continue processing other records instead of failing completely
            continue

    return {"ok": True, "processed": processed_count}
