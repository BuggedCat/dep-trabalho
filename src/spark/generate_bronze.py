import boto3
from botocore.exceptions import ClientError

from src.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_ENDPOINT_URL,
    AWS_SECRET_ACCESS_KEY,
    LOCAL_DATA_DIR,
    DatalakeZones,
)

FILE_NAME = "covid_vacinacao_sp.csv"
LOCAL_FILE_PATH = (LOCAL_DATA_DIR / FILE_NAME).as_posix()
BRONZE_DATA_PREFIX = f"vacinacao_covid/{FILE_NAME}"

if __name__ == "__main__":
    try:
        s3_client = boto3.client(
            service_name="s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=AWS_ENDPOINT_URL,
        )
        s3_client.upload_file(
            LOCAL_FILE_PATH,
            DatalakeZones.BRONZE,
            BRONZE_DATA_PREFIX,
        )
    except ClientError:
        raise
