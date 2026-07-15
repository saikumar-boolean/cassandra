import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="RMA",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_return_material_charge_tab"]
)
def return_material_charge(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/return_material_charge_tab/",
                                           columns_to_drop=["credited_qty", "tax_class_id", "rma_line_no", "charge_percent_basis", "base_charge_percent_basis",
                                            "delivery_type", "tax_calc_structure_id", "customer_tax_usage_type", "hsn_sac_code"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="RMA",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_return_material_line_tab"]
)
def return_material_line(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/return_material_line_tab/",
                                           columns_to_drop=["replacement_order_no", "replacement_line_no", "replacement_rel_no", "replacement_line_item_no",
                                            "tax_class_id", "condition_code", "delivery_type", "rental_end_date", "tax_calc_structure_id",
                                            "catch_qty", "customer_tax_usage_type", "arrival_date", "hsn_sac_code"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="RMA",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_return_material_tab"]
)
def return_material(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/return_material_tab/",
                                           columns_to_drop=[
                                            "case_id", "task_id", "shipment_id", "ship_addr_county", "return_addr_county", "ship_address3",
                                            "ship_address4", "ship_address5", "ship_address6", "return_address3", "return_address4",
                                            "return_address5", "return_address6", "customer_tax_usage_type"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df