import json
from typing import Any, Dict, Optional, Tuple

ALLOWED_EVENT_TYPES = {
    "video_start",
    "watch_tick",
    "video_stop",
    "visibility_change",
    "player_state_change",
    "context_missing",
}

BASE_REQUIRED_FIELDS = [
    "schema",
    "event_id",
    "event_ts",
    "event_type",
    "client_session_id",
    "tab_id",
]

REQUIRED_FIELDS_BY_TYPE = {
    "video_start": ["video_id", "video_session_id"],
    "watch_tick": ["video_id", "video_session_id", "watch_ms_delta"],
    "video_stop": ["video_id", "video_session_id"],
    "visibility_change": ["is_visible"],
    "player_state_change": ["new_state"],
    "context_missing": ["context_type"],
}

def _is_nonempty_str(x: Any) -> bool:
    return isinstance(x, str) and len(x.strip()) > 0

def _is_int(x: Any) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)

def validate_event(event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    for f in BASE_REQUIRED_FIELDS:
        if f not in event:
            return False, f"missing {f}"

    if event.get("schema") != 1:
        return False, "unsupported schema"

    if not _is_nonempty_str(event.get("event_id")):
        return False, "invalid event_id"

    if not _is_nonempty_str(event.get("client_session_id")):
        return False, "invalid client_session_id"

    if not _is_nonempty_str(event.get("tab_id")):
        return False, "invalid tab_id"

    if not _is_int(event.get("event_ts")):
        return False, "event_ts must be int (epoch ms)"

    etype = event.get("event_type")
    if etype not in ALLOWED_EVENT_TYPES:
        return False, "invalid event_type"

    for f in REQUIRED_FIELDS_BY_TYPE.get(etype, []):
        if f not in event:
            return False, f"missing {f} for {etype}"

    if etype in ("video_start", "watch_tick", "video_stop"):
        if not _is_nonempty_str(event.get("video_id")):
            return False, "video_id must be non-empty string"
        if not _is_nonempty_str(event.get("video_session_id")):
            return False, "video_session_id must be non-empty string"

    if etype == "watch_tick":
        delta = event.get("watch_ms_delta")
        if not _is_int(delta) or delta < 0 or delta > 60_000:
            return False, "watch_ms_delta must be int 0..60000"

        wm = event.get("watch_mode")
        if wm is not None and wm not in ("foreground", "background"):
            return False, "watch_mode must be foreground|background (or omitted)"

    if etype == "visibility_change":
        if not isinstance(event.get("is_visible"), bool):
            return False, "is_visible must be boolean"

    return True, None

def parse_event_dict(event_dict: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    ok, err = validate_event(event_dict)
    if not ok:
        return None, err
    return event_dict, None

def parse_event_line(line: str) -> Optional[Dict[str, Any]]:
    line = line.strip()
    if not line:
        return None
    try:
        event_dict = json.loads(line)
    except json.JSONDecodeError:
        return None
    ok, _ = validate_event(event_dict)
    return event_dict if ok else None
