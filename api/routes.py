from flask import request, jsonify
from schema import parse_event_dict
from firehose_client import add_event, flush
from storage import append_local_ndjson
from config import FIREHOSE_STREAM_NAME, AWS_REGION


def register_routes(app):
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
                print(f"Rejected idx={idx}: not an object")
                continue

            parsed, err = parse_event_dict(ev)
            if parsed is None:
                rejected += 1
                errors.append({"index": idx, "error": err or "invalid event"})
                print(f"Rejected idx={idx}: {err} type={ev.get('event_type')}")
                continue

            print(f"Accepted idx={idx}: {parsed.get('event_type')} vid={parsed.get('video_id')} ch={parsed.get('channel_name')} delta={parsed.get('watch_ms_delta')}")

            append_local_ndjson(parsed)
            add_event(parsed)
            accepted += 1

        try:
            flush(force=True)
        except Exception as e:
            print("Firehose ERROR:", repr(e))
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

