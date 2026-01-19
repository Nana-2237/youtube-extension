import os
from dotenv import load_dotenv

load_dotenv()

FIREHOSE_STREAM_NAME = os.getenv("FIREHOSE_STREAM_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def validate_config():
    if not FIREHOSE_STREAM_NAME:
        raise ValueError("FIREHOSE_STREAM_NAME environment variable is required")
    if not AWS_REGION:
        raise ValueError("AWS_REGION environment variable is required")

DATA_DIR = "data"
EVENTS_FILE = os.path.join(DATA_DIR, "events.ndjson")
os.makedirs(DATA_DIR, exist_ok=True)

BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "50"))
BATCH_FLUSH_MS = int(os.getenv("BATCH_FLUSH_MS", "500"))

FLASK_HOST = os.getenv("FLASK_HOST", "127.0.0.1")
FLASK_PORT = int(os.getenv("FLASK_PORT", "4000"))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "False").lower() == "true"

