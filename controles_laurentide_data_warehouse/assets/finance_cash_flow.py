from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    name="bank_balance",
    group_name="Finance_cash_flow",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cash_account_balance_tab"]
)
def bank_balance(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/cash_account_balance_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="cash_account_trans_types",
    group_name="Finance_cash_flow",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cash_account_trans_types_tab"]
)
def cash_account_trans_types(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/cash_account_trans_types_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df

