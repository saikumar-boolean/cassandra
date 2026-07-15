from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df
import pandas as pd

@asset(
    name="labor_class",
    group_name="shop_order_set_up",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_labor_class_tab"]
)
def labor_class(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/labor_class_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
        "CODE_PART", "COST_CENTER_ID", "NOTE_TEXT", "CAPACITY_CALC_BASE", "CAPACITY_CALC_END_DATE"
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
