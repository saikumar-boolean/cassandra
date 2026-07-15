from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df
import pandas as pd

@asset(
    name="markets",
    group_name="Sales",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_sales_market_tab"]
)
def markets(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/sales_market_tab/",
        s3
    ).rename(columns=str.upper)

    columns_to_drop = [
        "ROWKEY"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="sales_group",
    group_name="Sales",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_sales_group_tab"]
)
def sales_group(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/sales_group_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df

