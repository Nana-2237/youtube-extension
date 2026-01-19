import json
from config import EVENTS_FILE


def append_local_ndjson(event_dict: dict) -> None:
    with open(EVENTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event_dict, ensure_ascii=False) + "\n")

