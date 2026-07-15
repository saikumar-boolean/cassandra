import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Part_Serial",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_part_serial_catalog_tab"]
)
def part_serial_catalog(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/part_serial_catalog_tab/",
                                           columns_to_drop=['serial_revision', 'warranty_expires', 'cust_warranty_id', 
                                                            'sup_warranty_id', 'date_locked', 'condition_code', 'eng_part_revision', 
                                                            'owner_id', 'partial_disassembly_level', 'owning_vendor_no', 
                                                            'fa_object_company', 'fa_object_id'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Part_Serial",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_part_serial_history_tab"]
)
def part_serial_catalog_history(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/part_serial_history_tab/",
                                           columns_to_drop=['superior_alternate_contract', 'eng_part_revision', 'acquisition_cost', 
                                                            'currency_code', 'owner_id', 'alternate_id', 'alternate_contract', 
                                                            'owning_vendor_no', 'partial_disassembly_level', 'partial_source_order_no', 
                                                            'partial_source_release_no', 'partial_source_seq_no', 'partial_dest_order_no', 
                                                            'partial_dest_release_no', 'partial_dest_seq_no', 'fa_object_company', 'fa_object_id'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df