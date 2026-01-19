from flask import Flask, request, jsonify
import os
import json
import boto3
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from schema import parse_event_dict

# Load environment variables from .env file
load_dotenv()

# ----- CONFIG -----
FIREHOSE_STREAM_NAME = os.getenv("FIREHOSE_STREAM_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Validate required environment variables
if not FIREHOSE_STREAM_NAME:
    raise ValueError("FIREHOSE_STREAM_NAME environment variable is required")
if not AWS_REGION:
    raise ValueError("AWS_REGION environment variable is required")

# For local debugging
DATA_DIR = "data"
EVENTS_FILE = os.path.join(DATA_DIR, "events.ndjson")
os.makedirs(DATA_DIR, exist_ok=True)

# Firehose batching (safer + cheaper)
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "50"))   # Firehose limit is 500
BATCH_FLUSH_MS = int(os.getenv("BATCH_FLUSH_MS", "500"))        # flush at least every 0.5s

# Flask server config
FLASK_HOST = os.getenv("FLASK_HOST", "127.0.0.1")
FLASK_PORT = int(os.getenv("FLASK_PORT", "4000"))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "False").lower() == "true"

# Initialize boto3 client with credentials from environment
firehose_kwargs = {"region_name": AWS_REGION}
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    firehose_kwargs.update({
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY
    })
firehose = boto3.client("firehose", **firehose_kwargs)

app = Flask(__name__)

# ----- In-memory batch buffer -----
_batch = []
_last_flush = 0.0


def _append_local_ndjson(event_dict: dict) -> None:
    with open(EVENTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event_dict, ensure_ascii=False) + "\n")


def _send_batch_to_firehose(events: list[dict]) -> None:
    """
    Send a batch of events to Firehose.
    Raises RuntimeError if any records fail.
    """
    records = [{"Data": (json.dumps(ev, ensure_ascii=False) + "\n").encode("utf-8")} for ev in events]
    
    resp = firehose.put_record_batch(
        DeliveryStreamName=FIREHOSE_STREAM_NAME,
        Records=records
    )
    
    failed = resp.get("FailedPutCount", 0)
    if failed:
        raise RuntimeError(f"Firehose batch failed: {failed} records failed. resp={resp}")


def _flush_firehose(force: bool = False) -> None:
    """
    Flush buffered records to Firehose with PutRecordBatch.
    Raises on failure so caller can return 500.
    """
    global _batch, _last_flush

    if not _batch:
        return

    now = time.time()
    if not force:
        # Flush if enough records or enough time elapsed
        if len(_batch) < BATCH_MAX_RECORDS and (now - _last_flush) * 1000 < BATCH_FLUSH_MS:
            return

    events = _batch
    _batch = []
    _last_flush = now

    try:
        _send_batch_to_firehose(events)
    except (ClientError, RuntimeError) as e:
        # Put the events back so you don't lose them in memory
        _batch = events + _batch
        raise

    # Success logging (short)
    print(f"🔥 Firehose batch OK: sent={len(events)}")


@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "content-type"
    return resp


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "region": AWS_REGION,
        "stream": FIREHOSE_STREAM_NAME
    }), 200


@app.route("/ingest", methods=["OPTIONS"])
def ingest_options():
    return ("", 204)


@app.route("/ingest", methods=["POST"])
def ingest():
    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400

    # batch or single
    if isinstance(payload, dict) and "events" in payload and isinstance(payload["events"], list):
        incoming_events = payload["events"]
    else:
        incoming_events = [payload]

    accepted = 0
    rejected = 0
    errors = []

    print(f"=== /ingest called: events={len(incoming_events)} ===")

    for idx, ev in enumerate(incoming_events):
        if not isinstance(ev, dict):
            rejected += 1
            errors.append({"index": idx, "error": "event is not an object"})
            print(f"❌ Rejected idx={idx}: not an object")
            continue

        parsed, err = parse_event_dict(ev)
        if parsed is None:
            rejected += 1
            errors.append({"index": idx, "error": err or "invalid event"})
            print(f"❌ Rejected idx={idx}: {err} type={ev.get('event_type')}")
            continue

        # Log accept (short)
        print(f"✅ Accepted idx={idx}: {parsed.get('event_type')} vid={parsed.get('video_id')} ch={parsed.get('channel_name')} delta={parsed.get('watch_ms_delta')}")

        # local debug log
        _append_local_ndjson(parsed)

        # buffer for Firehose (store event dict, not Firehose record)
        global _batch
        _batch.append(parsed)
        accepted += 1

    # Try flushing after ingest (force flush so you see S3 sooner while testing)
    try:
        _flush_firehose(force=True)
    except Exception as e:
        print("🔥 Firehose ERROR:", repr(e))
        return jsonify({
            "ok": False,
            "error": "Firehose write failed",
            "firehose_stream": FIREHOSE_STREAM_NAME,
            "region": AWS_REGION,
            "accepted": accepted,
            "rejected": rejected,
            "errors": errors[:10]
        }), 500

    return jsonify({
        "ok": True,
        "accepted": accepted,
        "rejected": rejected,
        "errors": errors[:10],
        "firehose_stream": FIREHOSE_STREAM_NAME,
        "region": AWS_REGION
    }), 200


if __name__ == "__main__":
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)
