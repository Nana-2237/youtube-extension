def merge_dict_add(dst: dict, src: dict):
    for k, v in (src or {}).items():
        if not isinstance(v, (int, float)):
            continue
        dst[k] = dst.get(k, 0) + v


def aggregate_partials(partial_keys: list, bucket: str):
    from s3_operations import read_json

    total_ms_by_channel = {}
    total_ms_by_channel_fg = {}
    total_ms_by_channel_bg = {}
    total_ms_by_video = {}
    views_by_channel = {}
    views_by_video = {}

    for k in partial_keys:
        doc = read_json(bucket, k)
        totals = (doc.get("totals") or {})
        views = (doc.get("views") or {})

        merge_dict_add(total_ms_by_channel, totals.get("total_ms_by_channel"))
        merge_dict_add(total_ms_by_channel_fg, totals.get("total_ms_by_channel_fg"))
        merge_dict_add(total_ms_by_channel_bg, totals.get("total_ms_by_channel_bg"))
        merge_dict_add(total_ms_by_video, totals.get("total_ms_by_video"))
        merge_dict_add(views_by_channel, views.get("views_by_channel"))
        merge_dict_add(views_by_video, views.get("views_by_video"))

    return {
        "channels": total_ms_by_channel,
        "channels_fg": total_ms_by_channel_fg,
        "channels_bg": total_ms_by_channel_bg,
        "videos": total_ms_by_video,
        "views_channels": views_by_channel,
        "views_videos": views_by_video,
    }


def build_rows(aggregated: dict, dt: str):
    channel_rows = []
    all_channels = set(aggregated["channels"].keys()) | set(aggregated["views_channels"].keys())
    for ch in sorted(all_channels):
        channel_rows.append({
            "dt": dt,
            "channel": ch,
            "watch_ms": int(aggregated["channels"].get(ch, 0)),
            "watch_ms_fg": int(aggregated["channels_fg"].get(ch, 0)),
            "watch_ms_bg": int(aggregated["channels_bg"].get(ch, 0)),
            "views": int(aggregated["views_channels"].get(ch, 0)),
        })

    video_rows = []
    all_videos = set(aggregated["videos"].keys()) | set(aggregated["views_videos"].keys())
    for vid in sorted(all_videos):
        video_rows.append({
            "dt": dt,
            "video_id": vid,
            "watch_ms": int(aggregated["videos"].get(vid, 0)),
            "views": int(aggregated["views_videos"].get(vid, 0)),
        })

    return channel_rows, video_rows

