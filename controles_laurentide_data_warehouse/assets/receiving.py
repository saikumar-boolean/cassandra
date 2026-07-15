import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Receiving",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_purchase_receipt_tab"]
)
def purchase_receipt(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/purchase_receipt_tab/",
                                           columns_to_drop=["airway_bill_no"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Receiving",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_receipt_history_tab"]
)
def receipt_history(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/receipt_history_tab/",
                                           columns_to_drop=["input_uom_parameters"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Receiving",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_receipt_info_tab"]
)
def receipt_info(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/receipt_info_tab/",
                                           columns_to_drop=["source_ref4", "airway_bill_no"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df