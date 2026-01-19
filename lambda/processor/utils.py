import json
import gzip
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional
from urllib.parse import unquote_plus
from config import RAW_PREFIX


def safe_json_loads(line: str) -> Optional[Dict[str, Any]]:
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None


def day_partition_from_key_or_fallback(key: str) -> Tuple[str, str, str]:
    parts = key.split("/")
    if len(parts) >= 4 and parts[0] == RAW_PREFIX.rstrip("/"):
        yyyy, mm, dd = parts[1], parts[2], parts[3]
        if len(yyyy) == 4 and len(mm) == 2 and len(dd) == 2:
            return yyyy, mm, dd
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}", f"{now.month:02d}", f"{now.day:02d}"


def hash_key(key: str, etag: str = "") -> str:
    h = hashlib.sha256()
    h.update(key.encode("utf-8"))
    if etag:
        h.update(etag.encode("utf-8"))
    return h.hexdigest()[:16]


def decode_body(key: str, body_bytes: bytes) -> str:
    if key.endswith(".gz"):
        return gzip.decompress(body_bytes).decode("utf-8", errors="replace")
    return body_bytes.decode("utf-8", errors="replace")

