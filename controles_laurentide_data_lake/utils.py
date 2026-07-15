from pyarrow import fs
from pyarrow.parquet import ParquetFile
import boto3
import shutil
import tempfile
from pathlib import Path

def list_s3_parquet_keys(context, bucket: str, prefix: str, s3_client=None):
    s3_client = s3_client or boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    return keys

def count_rows_in_s3_parquet_pyarrow(context, bucket: str, prefix: str,
                                     aws_access_key_id=None, aws_secret_access_key=None,
                                     region=None):
    # list parquet object keys under prefix
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region,
    )
    parquet_keys = list_s3_parquet_keys(context, bucket, prefix, s3_client=s3_client)
    if not parquet_keys:
        return 0

    # create pyarrow S3 filesystem (uses credentials provided or env profile)
    s3 = fs.S3FileSystem(
        region=region,
        access_key=aws_access_key_id,
        secret_key=aws_secret_access_key
    )

    total_rows = 0
    for key in parquet_keys:
        pa_path = f"{bucket}/{key}"   # pyarrow expects "bucket/key" style
        with s3.open_input_file(pa_path) as fh:
            pf = ParquetFile(fh)
            total_rows += pf.metadata.num_rows
    return total_rows

def _get_temp_pipelines_root() -> Path:
    """Cross-platform base directory for dlt pipelines."""
    root = Path(tempfile.gettempdir()) / "dlt_pipelines"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _cleanup_pipeline_dir(pipelines_root: Path, pipeline_name: str) -> None:
    """Delete the directory created for a given pipeline."""
    pipeline_dir = pipelines_root / pipeline_name
    shutil.rmtree(pipeline_dir, ignore_errors=True)