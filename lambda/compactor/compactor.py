from config import get_bucket
from utils import dt_today_utc, prefix_for_partials, prefix_for_out
from s3_operations import list_keys, write_jsonl
from aggregator import aggregate_partials, build_rows


def lambda_handler(event, context):
    bucket = get_bucket()
    dt = (event or {}).get("dt") or dt_today_utc()
    partials_prefix = prefix_for_partials(dt)
    partial_keys = list_keys(bucket, partials_prefix)

    if not partial_keys:
        print(f"No partials found for dt={dt} under {partials_prefix}")
        return {"ok": True, "dt": dt, "partials": 0, "written": False}

    aggregated = aggregate_partials(partial_keys, bucket)
    channel_rows, video_rows = build_rows(aggregated, dt)

    out_ch_key = prefix_for_out("channel_daily", dt) + "data.jsonl"
    out_vid_key = prefix_for_out("video_daily", dt) + "data.jsonl"

    write_jsonl(bucket, out_ch_key, channel_rows)
    write_jsonl(bucket, out_vid_key, video_rows)

    print(f"Wrote {len(channel_rows)} channel rows to s3://{bucket}/{out_ch_key}")
    print(f"Wrote {len(video_rows)} video rows to s3://{bucket}/{out_vid_key}")

    return {"ok": True, "dt": dt, "partials": len(partial_keys), "channel_rows": len(channel_rows), "video_rows": len(video_rows)}
