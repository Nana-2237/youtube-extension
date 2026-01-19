import boto3
import json
import os
from datetime import datetime, timezone

# Initialize S3 client (uses IAM role credentials in Lambda)
s3 = boto3.client("s3")

# Get configuration from environment variables (required in Lambda)
BUCKET = os.getenv("BUCKET")
PARTIALS_PREFIX = os.getenv("PARTIALS_PREFIX", "results/daily")
OUT_PREFIX = os.getenv("OUT_PREFIX", "analytics")

# Validate required environment variables
if not BUCKET:
    raise ValueError("BUCKET environment variable is required")

def _dt_today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _prefix_for_partials(dt: str) -> str:
    yyyy, mm, dd = dt.split("-")
    return f"{PARTIALS_PREFIX}/{yyyy}/{mm}/{dd}/partials/"

def _prefix_for_out(table: str, dt: str) -> str:
    return f"{OUT_PREFIX}/{table}/dt={dt}/"

def _list_keys(bucket: str, prefix: str):
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if not k.endswith("/"):
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def _read_json(bucket: str, key: str) -> dict:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))

def _merge_dict_add(dst: dict, src: dict):
    for k, v in (src or {}).items():
        if not isinstance(v, (int, float)):
            continue
        dst[k] = dst.get(k, 0) + v

def _write_jsonl(bucket: str, key: str, rows: list):
    data = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

def lambda_handler(event, context):
    dt = (event or {}).get("dt") or _dt_today_utc()

    partials_prefix = _prefix_for_partials(dt)
    partial_keys = _list_keys(BUCKET, partials_prefix)

    if not partial_keys:
        print(f"No partials found for dt={dt} under {partials_prefix}")
        return {"ok": True, "dt": dt, "partials": 0, "written": False}

    total_ms_by_channel = {}
    total_ms_by_channel_fg = {}
    total_ms_by_channel_bg = {}
    total_ms_by_video = {}
    views_by_channel = {}
    views_by_video = {}

    for k in partial_keys:
        doc = _read_json(BUCKET, k)

        totals = (doc.get("totals") or {})
        views = (doc.get("views") or {})

        _merge_dict_add(total_ms_by_channel, totals.get("total_ms_by_channel"))
        _merge_dict_add(total_ms_by_channel_fg, totals.get("total_ms_by_channel_fg"))
        _merge_dict_add(total_ms_by_channel_bg, totals.get("total_ms_by_channel_bg"))
        _merge_dict_add(total_ms_by_video, totals.get("total_ms_by_video"))

        _merge_dict_add(views_by_channel, views.get("views_by_channel"))
        _merge_dict_add(views_by_video, views.get("views_by_video"))

    channel_rows = []
    all_channels = set(total_ms_by_channel.keys()) | set(views_by_channel.keys())
    for ch in sorted(all_channels):
        channel_rows.append({
            "dt": dt,
            "channel": ch,
            "watch_ms": int(total_ms_by_channel.get(ch, 0)),
            "watch_ms_fg": int(total_ms_by_channel_fg.get(ch, 0)),
            "watch_ms_bg": int(total_ms_by_channel_bg.get(ch, 0)),
            "views": int(views_by_channel.get(ch, 0)),
        })

    video_rows = []
    all_videos = set(total_ms_by_video.keys()) | set(views_by_video.keys())
    for vid in sorted(all_videos):
        video_rows.append({
            "dt": dt,
            "video_id": vid,
            "watch_ms": int(total_ms_by_video.get(vid, 0)),
            "views": int(views_by_video.get(vid, 0)),
        })

    out_ch_key = _prefix_for_out("channel_daily", dt) + "data.jsonl"
    out_vid_key = _prefix_for_out("video_daily", dt) + "data.jsonl"

    _write_jsonl(BUCKET, out_ch_key, channel_rows)
    _write_jsonl(BUCKET, out_vid_key, video_rows)

    print(f"Wrote {len(channel_rows)} channel rows to s3://{BUCKET}/{out_ch_key}")
    print(f"Wrote {len(video_rows)} video rows to s3://{BUCKET}/{out_vid_key}")

    return {"ok": True, "dt": dt, "partials": len(partial_keys), "channel_rows": len(channel_rows), "video_rows": len(video_rows)}
