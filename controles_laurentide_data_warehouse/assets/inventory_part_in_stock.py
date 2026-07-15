import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    name="Inventory_part_in_stock_tab",
    group_name="Inventory",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_inventory_part_in_stock_tab"]
)
def inventory_part_in_stock(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/inventory_part_in_stock_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
        "DEPARTMENT", 
        "DEL_TYPE", 
        "LOCATION_CLASS", 
        "LOW_LEVEL_CODE", 
        "ROTABLE_PART_POOL_ID", 
        "CATCH_QTY_IN_TRANSIT", 
        "CATCH_QTY_ONHAND", 
        "OWNING_VENDOR_NO"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
