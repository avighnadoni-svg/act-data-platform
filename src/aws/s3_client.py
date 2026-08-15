import io
import os

import boto3
import pandas as pd


BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")


def upload_json_as_csv_to_s3(
    data: list[dict],
    entity_name: str
):

    if not BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is missing")

    if not data:
        print(f"No data received for {entity_name}")
        return None

    # JSON -> DataFrame
    df = pd.DataFrame(data)

    # DataFrame -> CSV in memory
    csv_buffer = io.StringIO()

    df.to_csv(
        csv_buffer,
        index=False
    )

    # S3 object path
    s3_key = f"act/raw/{entity_name}/{entity_name}.csv"

    # Boto3 automatically reads AWS credentials
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )

    s3_path = f"s3://{BUCKET_NAME}/{s3_key}"

    print(
        f"{len(df)} records uploaded to {s3_path}"
    )

    return s3_path