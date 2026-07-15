import pandas as pd
from dagster import asset, EnvVar, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    name="inventory_transactions",
    group_name="Inventory",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "ifs_inventory_transaction_cost_tab",
        "ifs_inventory_transaction_hist_tab"
        # "ifs_inventory_transaction_hist_cft" removed
    ]
)
def inventory_transactions(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    cost = generic_extraction_and_cleaning(context, "ifs/inventory_transaction_cost_tab/", s3).rename(columns=str.upper)
    hist = generic_extraction_and_cleaning(context, "ifs/inventory_transaction_hist_tab/", s3).rename(columns=str.upper)


    hist = hist.drop(columns=[
    col for col in ["CONTRACT", "ROWKEY", "ROWVERSION"] if col in hist.columns])

    df = cost.merge(hist, on="TRANSACTION_ID", how="left", suffixes=("", "_hist"))

    columns_to_drop = [
        "BUCKET_POSTING_GROUP_ID", "LEVEL_UNIT_COST", "DEL_TYPE", "OWNING_VENDOR_NO",
        "OWNING_CUSTOMER_NO", "CONDITION_CODE", "DELIVER_OVERHEAD", "CATCH_QUANTITY",
        "CATCH_DIRECTION", "PALLET_ID", "TRANSACTION_REPORT_ID", "ORIGINAL_AMOUNT",
        "DELIVERY_REASON_ID", "ALT_DEL_NOTE_NO", "DEL_NOTE_DATE", "PART_MOVE_TAX_ID"
        # Removed "ROWKEY_2", "CF$_MANUAL_ADJUSTMENT_NOTE" since they came from CFT
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
