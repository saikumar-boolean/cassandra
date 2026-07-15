import os
from typing import Callable
import dlt
from dagster_dlt import DagsterDltResource
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database

from .dlt_sources.salesforce import salesforce_source
from dagster import asset, AutomationCondition, AssetExecutionContext, AssetKey
from dlt.destinations import filesystem
from dlt.common.configuration.specs import AwsCredentials 
from datetime import datetime, timedelta, timezone
from .utils import count_rows_in_s3_parquet_pyarrow, _get_temp_pipelines_root, _cleanup_pipeline_dir

dlt_resource = DagsterDltResource()

IFS_CREDENTIALS = ConnectionStringCredentials(os.getenv("IFS_CONNECTION_STRING"))

SF_USERNAME = os.getenv("SALESFORCE_USERNAME")
SF_PASSWORD = os.getenv("SALESFORCE_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SALESFORCE_SECURITY_TOKEN")
SF_INSTANCE_URL = os.getenv("SALESFORCE_INSTANCE_URL")

DATA_LAKE_BUCKET_NAME = os.getenv("DATA_LAKE_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("WAREHOUSE_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("WAREHOUSE_AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("WAREHOUSE_AWS_REGION", "us-east-1")

PIPELINES_CRON = os.getenv("PIPELINES_CRON")

def generate_ifs_asset(name: str) -> Callable:
    @asset(
        pool="ifs",
        name=f"ifs_{name.lower()}",
        group_name="ifs_tables",
        description=f"IFS Table {name.lower()}",
        kinds={"dlt", "parquet", "oracle"},
        automation_condition=AutomationCondition.on_cron(PIPELINES_CRON, 'America/New_York')
    )
    def _ifs_asset():

        asset_name = f"ifs_{name.lower()}"
        pipeline_name = f"{asset_name}_pipeline"
        source = sql_database(
            credentials=IFS_CREDENTIALS,
            table_names=[name.lower()],
            resolve_foreign_keys=False
        )
        s3_credentials = AwsCredentials(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_destination = filesystem(
            bucket_url=f"s3://{DATA_LAKE_BUCKET_NAME}", 
            credentials=s3_credentials
        )
        
        pipelines_root = _get_temp_pipelines_root()
        print(f"DLT temp folder for pipeline '{pipeline_name}': {pipelines_root / pipeline_name}")
        
        pipeline = dlt.pipeline(
            pipeline_name=f"ifs_{name.lower()}_pipeline",
            destination=s3_destination,
            dataset_name="ifs",
            progress="log",
            pipelines_dir=str(pipelines_root),
        )

        try:
            load_info = pipeline.run(
                source,
                loader_file_format="parquet",
                write_disposition="replace",
            )
            print(load_info)
        finally:
            _cleanup_pipeline_dir(pipelines_root, pipeline_name)
        print(load_info)

    return _ifs_asset


def generate_salesforce_asset(name: str) -> Callable:

    @asset(
        name=f"salesforce_{name.lower()}",
        group_name="salesforce_tables",
        description=f"Salesforce Table {name}",
        pool="salesforce",
        kinds={"dlt", "parquet", "salesforce"},
        automation_condition=AutomationCondition.on_cron(PIPELINES_CRON, 'America/New_York')
    )
    def _salesforce_asset():
        asset_name = f"salesforce_{name.lower()}"
        pipeline_name = f"{asset_name}_pipeline"
        source = salesforce_source(
            table_name=name,
            user_name=SF_USERNAME,
            password=SF_PASSWORD,
            security_token=SF_SECURITY_TOKEN,
        )
        s3_credentials = AwsCredentials(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_destination = filesystem(
            bucket_url=f"s3://{DATA_LAKE_BUCKET_NAME}", 
            credentials=s3_credentials
        )
        pipelines_root = _get_temp_pipelines_root()
        print(f"DLT temp folder for pipeline '{pipeline_name}': {pipelines_root / pipeline_name}")

        pipeline = dlt.pipeline(
            pipeline_name=f"sf_{name.lower()}_pipeline",
            destination=s3_destination,
            dataset_name="salesforce",
            progress="log"
        )

        try:
            load_info = pipeline.run(
                source,
                loader_file_format="parquet",
                write_disposition="replace",
            )
            print(load_info)
        finally:
            _cleanup_pipeline_dir(pipelines_root, pipeline_name)

    return _salesforce_asset

def generate_ifs_asset_with_offset(name: str) -> Callable:
    @asset(
        pool="ifs",
        name=f"ifs_{name.lower()}",
        group_name="ifs_tables",
        description=f"IFS Table {name.lower()} with row offset tracking",
        kinds={"dlt", "parquet", "oracle"},
        automation_condition=AutomationCondition.on_cron(PIPELINES_CRON, 'America/New_York')
    )
    def _ifs_asset(context: AssetExecutionContext):
        asset_name = f"ifs_{name.lower()}"
        pipeline_name = f"{asset_name}_pipeline"

        # Get the last materialization to find where we left off
        last_materialization = context.instance.get_latest_materialization_event(
            AssetKey([asset_name])
        )

        # Get the total rows processed from previous run
        rows_already_processed = 0
        if last_materialization:
            materialization = getattr(
                last_materialization.dagster_event.event_specific_data, "materialization", None
            )
            if materialization and hasattr(materialization, "metadata"):
                total_rows_md = materialization.metadata.get("total_rows_processed")
                if total_rows_md:
                    rows_already_processed = total_rows_md.value

        context.log.info(f"Previous runs processed {rows_already_processed} rows")

        # SQLAlchemy query adapter for incremental load
        def make_offset_adapter(offset: int):
            def adapter(select_stmt, table, engine=None):
                context.log.info(f"Generated SQL: {select_stmt}")
                if offset > 0:
                    context.log.info(f"Running incremental load, skipping first {offset} rows")
                    return select_stmt.offset(offset)
                else:
                    context.log.info("Running initial full load")
                    return select_stmt
            return adapter

        # Define source with SQLAlchemy adapter
        source = sql_database(
            credentials=IFS_CREDENTIALS,
            resolve_foreign_keys=False,
            table_names=[name.lower()],
            query_adapter_callback=make_offset_adapter(rows_already_processed)
        )

        # S3 destination
        s3_credentials = AwsCredentials(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_destination = filesystem(
            bucket_url=f"s3://{DATA_LAKE_BUCKET_NAME}",
            credentials=s3_credentials
        )

        pipelines_root = _get_temp_pipelines_root()

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=s3_destination,
            dataset_name="ifs",
            progress="log",
            pipelines_dir=str(pipelines_root),
        )

        # Determine write disposition
        write_disposition = "append" if rows_already_processed > 0 else "replace"

        try:
            load_info = pipeline.run(
                source,
                loader_file_format="parquet",
                write_disposition=write_disposition,
            )

            new_total_rows = count_rows_in_s3_parquet_pyarrow(
                context, DATA_LAKE_BUCKET_NAME, f"ifs/{name.lower()}",
                AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
            
            context.log.info(f"new_total_rows: {new_total_rows}")

            # Store metadata for next run
            context.add_output_metadata({
                "total_rows_processed": new_total_rows,
                "rows_added_this_run": new_total_rows - rows_already_processed,
                "write_disposition": write_disposition,
                "execution_timestamp": datetime.now().isoformat()
            })

        finally:
            _cleanup_pipeline_dir(pipelines_root, pipeline_name)

        return load_info

    return _ifs_asset