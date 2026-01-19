import json
from s3_client import s3


def list_keys(bucket: str, prefix: str):
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if not k.endswith("/"):
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def read_json(bucket: str, key: str) -> dict:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))


def write_jsonl(bucket: str, key: str, rows: list):
    data = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

