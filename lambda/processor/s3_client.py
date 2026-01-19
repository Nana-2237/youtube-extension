import boto3
from botocore.config import Config

s3_config = Config(
    max_pool_connections=50,
    retries={'max_attempts': 3, 'mode': 'standard'},
    connect_timeout=60,
    read_timeout=60
)
s3 = boto3.client("s3", config=s3_config)

