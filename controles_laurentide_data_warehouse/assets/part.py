from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, rename_columns, map_column_values, add_surrogate_key, ensure_snowflake_schema_matches_df
@asset(
    group_name="Part",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_part_catalog_tab",
          "ifs_part_catalog_cft",
          "ifs_product_code_x_ref_clt",
          "ifs_code_b"]
)
def part(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_part_catalog_tab = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/part_catalog_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[]
    )
    keep_cols_part_catalog_tab = ['part_no', 'description', 'part_main_group', 'info_text', 'unit_code', 'serial_tracking_code', 
                                  'eng_serial_tracking_code', 'serial_rule', 'lot_tracking_code', 'configurable', 'lot_quantity_rule', 
                                  'sub_lot_rule', 'multilevel_tracking', 'component_lot_rule', 'stop_arrival_issued_serial', 
                                  'allow_as_not_consumed', 'weight_net', 'volume_net', 'uom_for_weight_net', 'uom_for_volume_net', 
                                  'receipt_issue_serial_track', 'stop_new_serial_in_rma', 'c_manufacturer', 'c_manuf_part_no', 
                                  'c_list_price', 'c_deal_no', 'c_application_code', 'c_effective_price_date', 'c_division_id', 
                                  'c_discount', 'c_commission_percent', 'c_display_factor', 'c_line_type', 'c_branch_plant', 
                                  'c_global_inventory', 'c_exclude_reprice', 'c_create_user', 'c_create_date', 'c_modify_user', 
                                  'c_modify_date', 'c_part_product_code', 'c_product_group', 'c_product_line', 'rowkey']
    
    retrieve_from_s3_part_catalog_tab = retrieve_from_s3_part_catalog_tab[keep_cols_part_catalog_tab]
    retrieve_from_s3_part_catalog_tab = rename_columns(df=retrieve_from_s3_part_catalog_tab, old_cols=keep_cols_part_catalog_tab,
                                                           new_cols=['PART_NO', 'DESCRIPTION', 'PART_MAIN_GROUP', 'INFO_TEXT', 
                                                                     'UNIT_CODE', 'SERIAL_TRACKING_CODE', 'ENG_SERIAL_TRACKING_CODE', 
                                                                     'SERIAL_RULE', 'LOT_TRACKING_CODE', 'CONFIGURABLE',
                                                                     'LOT_QUANTITY_RULE', 'SUB_LOT_RULE', 'MULTILEVEL_TRACKING',
                                                                     'COMPONENT_LOT_RULE', 'STOP_ARRIVAL_ISSUED_SERIAL',
                                                                     'ALLOW_AS_NOT_CONSUMED', 'WEIGHT_NET', 'VOLUME_NET',
                                                                     'UOM_FOR_WEIGHT_NET', 'UOM_FOR_VOLUME_NET',
                                                                     'RECEIPT_ISSUE_SERIAL_TRACK', 'STOP_NEW_SERIAL_IN_RMA',
                                                                     'C_MANUFACTURER', 'C_MANUF_PART_NO', 'C_LIST_PRICE', 'C_DEAL_NO',
                                                                     'C_APPLICATION_CODE', 'C_EFFECTIVE_PRICE_DATE', 'C_DIVISION_ID',
                                                                     'C_DISCOUNT', 'C_COMMISSION_PERCENT', 'C_DISPLAY_FACTOR',
                                                                     'C_LINE_TYPE', 'C_BRANCH_PLANT', 'C_GLOBAL_INVENTORY',
                                                                     'C_EXCLUDE_REPRICE', 'C_CREATE_USER', 'C_CREATE_DATE',
                                                                     'C_MODIFY_USER', 'C_MODIFY_DATE', 'C_PART_PRODUCT_CODE',
                                                                     'C_PRODUCT_GROUP', 'C_PRODUCT_LINE', 'ROWKEY'])
    
    retrieve_from_s3_product_code_x_ref_clt = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/product_code_x_ref_clt/",
        s3 = context.resources.s3_client,
        columns_to_drop=[]
    )

    keep_cols_product_code_x_ref_clt = ['cf_product_code', 'cf_sales_group', 'cf_sales_price_group', 
                                        'rowversion', 'cf_lcl_bu', 'cf_lcl_category_1', 'cf_lcl_category_2']
    
    retrieve_from_s3_product_code_x_ref_clt = retrieve_from_s3_product_code_x_ref_clt[keep_cols_product_code_x_ref_clt]


    retrieve_from_s3_product_code_x_ref_clt = rename_columns(df=retrieve_from_s3_product_code_x_ref_clt, old_cols=keep_cols_product_code_x_ref_clt, 
                   new_cols=['PRODUCT_CODE', 'SALES_GROUP', 'SALES_PRICE_GROUP', 'ROWVERSION', 'BUSINESS_UNIT_KEY', 'CATEGORY_1', 'CATEGORY_2'])
    
    retrieve_from_s3_part_catalog_cft = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/part_catalog_cft/",
        s3 = context.resources.s3_client,
        columns_to_drop=[]
    )

    keep_cols_part_catalog_cft = ['rowkey', 'cf_nomisid', 'cf_modelstring']
    
    retrieve_from_s3_part_catalog_cft = retrieve_from_s3_part_catalog_cft[keep_cols_part_catalog_cft]
    retrieve_from_s3_part_catalog_cft = rename_columns(df=retrieve_from_s3_part_catalog_cft, old_cols=keep_cols_part_catalog_cft, 
                   new_cols=['ROWKEY', 'NOMISID', 'MODELSTRING'])

    retrieve_from_s3_code_b = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/code_b/",
        s3 = context.resources.s3_client,
        columns_to_drop=[]
    )

    keep_cols_code_b = ['objkey', 'description']

    retrieve_from_s3_code_b = retrieve_from_s3_code_b[keep_cols_code_b]
    retrieve_from_s3_code_b = rename_columns(df=retrieve_from_s3_code_b, old_cols=keep_cols_code_b,
                   new_cols=['OBJKEY', 'BUSINESS_UNIT'])

    merged_df = pd.merge(
        retrieve_from_s3_part_catalog_tab,
        retrieve_from_s3_part_catalog_cft,
        on="ROWKEY", how="left",
        suffixes=("_part_catalog_tab", "_part_catalog_cft"))

    merged_df = pd.merge(
        merged_df,
        retrieve_from_s3_product_code_x_ref_clt,
        left_on="C_PART_PRODUCT_CODE", right_on="PRODUCT_CODE", how="left",
        suffixes=("", "_product_code_x_ref_clt"))

    merged_df = pd.merge(
        merged_df,
        retrieve_from_s3_code_b,
        left_on="BUSINESS_UNIT_KEY", right_on="OBJKEY", how="left",
        suffixes=("", "_code_b"))

    merged_df = add_surrogate_key(merged_df, key_name="SK_PART_ID", by=["PART_NO"])

    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df