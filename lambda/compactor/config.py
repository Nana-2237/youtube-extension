import os

BUCKET = os.getenv("BUCKET")
PARTIALS_PREFIX = os.getenv("PARTIALS_PREFIX", "results/daily")
OUT_PREFIX = os.getenv("OUT_PREFIX", "analytics")

def get_bucket():
    if not BUCKET:
        raise ValueError("BUCKET environment variable is required")
    return BUCKET

