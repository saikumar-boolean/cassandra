from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df
import pandas as pd

@asset(
    name="exp_code_per_exp_rule",
    group_name="Expense_BD",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_bkp_expense_code"]
)
def exp_code_per_exp_rule(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/bkp_expense_code/",
        s3
    ).rename(columns=str.upper)
    

    columns_to_drop = [
        "PERCENTAGE", "MINIMUM_PRICE", "MAXIMUM_PRICE", "ABROAD_RULE",
        "MILEAGE_RULE", "SUB_TOTAL_DESC", "AVAILABILITY",
        "VAT_PERCENTAGE", "GUEST_DETAIL_MANDATORY"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
