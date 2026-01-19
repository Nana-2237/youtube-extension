from datetime import datetime, timezone
from config import PARTIALS_PREFIX, OUT_PREFIX


def dt_today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def prefix_for_partials(dt: str) -> str:
    yyyy, mm, dd = dt.split("-")
    return f"{PARTIALS_PREFIX}/{yyyy}/{mm}/{dd}/partials/"


def prefix_for_out(table: str, dt: str) -> str:
    return f"{OUT_PREFIX}/{table}/dt={dt}/"

