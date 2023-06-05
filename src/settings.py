from enum import Enum
from pathlib import Path
from typing import Union

from decouple import config

AWS_ACCESS_KEY_ID = config(
    "AWS_ACCESS_KEY_ID",
    cast=str,
)
AWS_SECRET_ACCESS_KEY = config(
    "AWS_SECRET_ACCESS_KEY",
    cast=str,
)
AWS_ENDPOINT_URL = config(
    "AWS_ENDPOINT",
    cast=str,
)
SPARK_CONNECT_ENDPOINT = config(
    "SPARK_CONNECT_ENDPOINT",
    cast=str,
    default="sc://localhost",
)

LOCAL_DATA_DIR = Path("data")


class DatalakeZones(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

    def to_s3_uri(self, spark_fmt: bool = True, prefix: Union[str, None] = None):
        if spark_fmt:
            path = f"s3a://{self.value}"
        else:
            path = f"s3://{self.value}"

        if not prefix:
            return path
        return f"{path}/{prefix}"
