import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="opportunities",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["salesforce_opportunity"]
)
def opportunity(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="salesforce/opportunity/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="opportunities",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["salesforce_opportunitylineitem"]
)
def opportunitylineitem(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="salesforce/opportunity_line_item/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="opportunities",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["salesforce_opportunityhistory"]
)
def opportunityhistory(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="salesforce/opportunity_history/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df