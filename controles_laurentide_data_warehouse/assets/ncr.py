from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df
import pandas as pd

@asset(
    name="ncr",
    group_name="Ncr",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=[
        "non_conformance_report_tab",
        "ncr_corrective_action_tab",
        "ncr_object_connection_tab"
    ]
)
def ncr(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    ncr = generic_extraction_and_cleaning(context, "ifs/non_conformance_report_tab/", s3).rename(columns=str.upper)
    action = generic_extraction_and_cleaning(context, "ifs/ncr_corrective_action_tab/", s3).rename(columns=str.upper)
    connection = generic_extraction_and_cleaning(context, "ifs/ncr_object_connection_tab/", s3).rename(columns=str.upper)

    action = action.drop(columns=[
    col for col in ["ROWKEY", "ROWSTATE","ROWVERSION","CONTRACT", "COMPANY", "NOTES", "DESCRIPTION", "CREATED_BY", "DATE_CREATED", "RESPONSIBLE_PERSON_ID", "ROOT_CAUSE_ID"] if col in action.columns])

    connection = connection.drop(columns=[
    col for col in ["ROWKEY", "ROWVERSION"] if col in connection.columns])

    df = ncr.merge(action, on="NCR_NO", how="left", suffixes=("", "_action"))
    df = df.merge(connection, on="NCR_NO", how="left", suffixes=("", "_conn"))

    columns_to_drop = [
        "NCR_REFERENCE_DETAILS", "REFERENCE_STANDARD", "CASE_ID", "TASK_ID"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
