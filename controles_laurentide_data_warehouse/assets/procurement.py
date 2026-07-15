import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_customer_order_reservation_tab"]
)
def customer_order_reservation(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/customer_order_reservation_tab/",
                                           columns_to_drop=["input_qty", "input_unit_meas", 
                                            "input_conv_factor", "input_variable_values", "catch_qty", 
                                            "catch_qty_to_deliver", 
                                            "preliminary_pick_list_no"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_customer_order_shop_order_tab"]
)
def customer_order_shop_order(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/customer_order_shop_order_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_customer_order_shortage_tab"]
)
def customer_order_shortage(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/customer_order_shortage_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_project_misc_procurement_tab"]
)
def project_misc_procurement(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/project_misc_procurement_tab/",
                                           columns_to_drop=[
                                            "no_part_description", "unit", "information", 
                                            "s_order_no", "s_release_no", "s_sequence_no", "dop_id",
                                            "assortment", "stat_grp", "dop_id_temp"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_purchase_req_line_tab"]
)
def purchase_requisition_lines(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/purchase_req_line_tab/",
                                           columns_to_drop=["process_type", "authorize_id", "date_assigned", "inventory_flag", "split_percentage", "requested_qty",
                                            "assortment", "technical_coordinator_id", "template_id", "job_id", "service_type", "lot_batch_no",
                                            "serial_no", "condition_code", "core_deposit_curr", "core_deposit_base", "project_cost_element",
                                            "kanban_receipt_location", "reject_date", "rejected_by", "reject_reason_id", "auth_reject_note",
                                            "core_deposit_incl_tax_curr", "core_deposit_incl_tax_base", "destination_warehouse_id",
                                            "tax_calc_structure_id", "internal_income_type", "hsn_sac_code"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df
@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_purchase_requisition_tab"]
)
def purchase_requisition(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/purchase_requisition_tab/",
                                           columns_to_drop=["authorize_id"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_purchase_transaction_hist_tab"]
)
def purchase_transactions(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/purchase_transaction_hist_tab/",
                                           columns_to_drop=["source", "associated_transaction_id", 
                                                            "alt_source_ref1", "alt_source_ref2", 
                                                            "alt_source_ref3", "alt_source_ref4", 
                                                            "alt_source_ref5", "alt_source_ref_type", 
                                                            "delivery_reason_id", "alt_del_note_no",
    "del_note_date"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_customer_order_pur_order_tab"]
)
def customer_order_pur_order(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/customer_order_pur_order_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_purchase_order_tab",
          "ifs_purchase_order_cft"]
)
def purchase_order_header(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_purchase_order_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/purchase_order_tab/",
        columns_to_drop=["approval_rule", "authorize_id", "addr_1", "addr_2", "forwarder_id", "addr_3", "addr_4", "addr_5",
        "case_id", "task_id", "template_id", "rejected_date", "rejected_by", "reject_reason_id", 
        "ext_transport_calendar_id", "total_amt_at_auth", "c_price_factor", "c_rep_order_id", "c_emerson_id",
        "c_ship_attention_line2", "c_third_party_bill", "c_domestic_forwarder", "c_loc_number",
        "c_enggconstruction", "c_business_segment", "c_admin_code", "c_kob2", "c_kob3", "c_loc_expiry_date",
        "c_bill_attention_line1", "c_bill_attention_line2", "c_customer_fax", "c_cc_authorization", "c_cc_receipt",
        "c_create_datetime", "c_create_user", "c_change_user", "c_change_datetime", "c_intermediate_party",
        "c_addl_party_1", "c_addl_party_2", "c_addl_party_3", "c_addl_party_4", "c_addl_party_5", "c_addl_party_6",
        "c_addl_party_7", "c_addl_party_8", "c_addl_party_9", "route_id", "address3", "address4", "address5", "address6"])
    
    retrieve_from_s3_purchase_order_cft = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/purchase_order_cft/")
    
    df = pd.merge(
        retrieve_from_s3_purchase_order_tab, 
        retrieve_from_s3_purchase_order_cft,
        on="rowkey", how="left", 
        suffixes=("_purchase_order_tab", "_purchase_order_cft"))

    ensure_snowflake_schema_matches_df(context, df)
    
    return df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_purchase_part_tab",
          "ifs_purchase_part_supplier_tab"]
)
def purchase_part(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_purchase_part_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/purchase_part_tab/",
        columns_to_drop=["process_type", "qc_code", "qc_date", 
        "technical_coordinator_id", "statistical_code", "statistical_code_manuf", 
        "quality_system_level_id", "qsl_approval_template", "qmr_approval_template",
        "qsr_approval_template"])
    
    retrieve_from_s3_purchase_part_supplier_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/purchase_part_supplier_tab/",
        columns_to_drop=["assortment", "delivery_pattern_id", "inspection_code", "del_type", 
        "warranty_id", "dist_order_coordinator", "fee_code", "template_id", 
        "packing_instruction_id", "internal_income_type", "hsn_sac_code"])
    
    df = pd.merge(
        retrieve_from_s3_purchase_part_tab, 
        retrieve_from_s3_purchase_part_supplier_tab,
        on=["part_no","contract"], how="left", 
        suffixes=("_purchase_part_tab", "_purchase_part_supplier_tab"))

    ensure_snowflake_schema_matches_df(context, df)
    
    return df

@asset(
    group_name="Procurement",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_purchase_order_line_cft",
          "ifs_purchase_order_line_part_cft",
          "ifs_purchase_order_line_tab"]
)
def purchase_order_lines(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_purchase_order_line_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/purchase_order_line_tab/",
        columns_to_drop=["process_type", "blanket_order", "blanket_line", "inspection_code", "qc_code", "delivery_control_code",
        "last_ord_conf_reminder", "last_delivery_reminder", "replaces_order_no", "replaces_line_no",
        "replaces_release_no", "replaces_receipt_no", "assortment", "standard_order_qty", "despatch_qty",
        "despatch_date", "delnote_no", "technical_coordinator_id", "job_id", "warranty_id",
        "outside_operation_notes", "condition_code", "service_type", "lot_batch_no", "serial_no",
        "original_part_no", "defect_part_key_ref", "purch_ship_back", "core_deposit", "core_deposit_base",
        "forwarder_id", "deliver_to_customer_no", "ean_location_del_addr", "project_cost_element",
        "delivery_note_ref", "kanban_receipt_location", "campaign_id", "srv_service_type",
        "ext_transport_calendar_id", "input_gtin_no", "input_unit_meas", "input_qty", "input_conv_factor",
        "customs_stat_no", "intrastat_conv_factor", "c_parent_line_no", "c_component_number",
        "c_special_price_id", "c_special_gid", "c_invalid_special", "c_emerson_line", "c_ship_to_number",
        "c_ship_method", "c_pre_carrier", "c_pre_carrier_frght_pay_term", "c_main_carrier",
        "c_main_carrier_frht_pay_term", "c_third_party_bill", "c_carrier_account_number", "c_inco_terms",
        "c_named_place", "c_tax_explanation_code", "c_ship_attention_line1", "c_ship_attention_line2",
        "c_shipment_priority", "destination_warehouse_id", "closest_agrmt_part_assort_line",
        "agrmt_part_assort_line_no", "route_id", "packing_instruction_id", "tax_calc_structure_id",
        "country_of_origin", "region_of_origin", "category_assortment", "category_assortment_node",
        "work_task_seq", "internal_income_type", "hsn_sac_code"])
    
    retrieve_from_s3_purchase_order_line_cft = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/purchase_order_line_cft/",
        columns_to_drop=["cf_customer_po_line_no", "cf_customer_po_rel_no", "cf_nan_axiom_send"])     
    
    retrieve_from_s3_purchase_order_line_part_cft = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/purchase_order_line_part_cft/",
        columns_to_drop=["cf_customer_po_no", "cf_nan_qt_tags"])

    df = pd.merge(
        retrieve_from_s3_purchase_order_line_tab, 
        retrieve_from_s3_purchase_order_line_cft,
        on="rowkey", how="left", 
        suffixes=("_purchase_order_line_tab", "_purchase_order_line_cft"))
    
    df = pd.merge(
        df, 
        retrieve_from_s3_purchase_order_line_part_cft,
        on="rowkey", how="left", 
        suffixes=("", "_purchase_order_line_part_cft"))

    ensure_snowflake_schema_matches_df(context, df)
    
    return df