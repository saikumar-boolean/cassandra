
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df
import pandas as pd

@asset(
    name="customer_value",
    group_name="customer_value",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["salesforce_customerstory__c"]
)
def customer_value(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "salesforce/customer_story__c/", s3).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df
