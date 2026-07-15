from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df
import pandas as pd

@asset(
    name="returns",
    group_name="returns",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_receipt_return_tab"]
)
def returns(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/receipt_return_tab/", s3).rename(columns=str.upper)

    columns_to_drop = ["SOURCE_REF4", "CATCH_QTY_RETURNED"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
