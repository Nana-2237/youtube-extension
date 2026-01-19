from typing import Any, Dict
from utils import safe_json_loads


def aggregate_ndjson(ndjson_text: str) -> Dict[str, Any]:
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
        ev = safe_json_loads(raw_line)
        if ev is None:
            invalid_events += 1
            continue

        etype = ev.get("event_type")
        ts = ev.get("event_ts")
        tab_id = ev.get("tab_id")

        if etype is None or ts is None or tab_id is None:
            invalid_events += 1
            continue

        valid_events += 1

        if etype == "video_start":
            vid = ev.get("video_id")
            ch = ev.get("channel_name")
            if vid:
                views_by_video[vid] = views_by_video.get(vid, 0) + 1
            if ch:
                views_by_channel[ch] = views_by_channel.get(ch, 0) + 1
            continue

        if etype != "watch_tick":
            continue

        delta = ev.get("watch_ms_delta")
        if not isinstance(delta, int) or delta <= 0:
            continue

        video_id = ev.get("video_id")
        channel = ev.get("channel_name")
        watch_mode = ev.get("watch_mode")

        if not video_id:
            ignored_no_video_ticks += 1
            continue

        total_ms_by_video[video_id] = total_ms_by_video.get(video_id, 0) + delta

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

