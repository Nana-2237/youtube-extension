import json
from typing import Any, Dict
from s3_client import s3


def write_json(bucket: str, key: str, obj: Dict[str, Any]) -> None:
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"Error writing to s3://{bucket}/{key}: {e}")
        raise


def move_raw_to_processed(bucket: str, raw_key: str, processed_key: str) -> None:
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": raw_key},
            Key=processed_key,
        )
        s3.delete_object(Bucket=bucket, Key=raw_key)
    except Exception as e:
        print(f"Error moving s3://{bucket}/{raw_key} to {processed_key}: {e}")
        raise

