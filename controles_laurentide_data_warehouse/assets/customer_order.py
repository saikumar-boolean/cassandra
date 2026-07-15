import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Customer_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_staged_billing_template_tab",
          "ifs_customer_order_cft",
          "ifs_customer_order_tab"]
)
def customer_order_header(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_customer_order_tab = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/customer_order_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
        "print_control_code", "forward_agent", "route_id", "forward_agent_id", "external_ref", "case_id",
        "task_id", "sales_contract_no", "contract_rev_seq", "contract_line_no", "contract_item_no",
        "expected_prepayment_date", "tax_id_validated_date", "internal_po_label_note", "classification_standard",
        "rebate_customer", "zone_id", "freight_map_id", "freight_price_list_no", "fix_deliv_freight",
        "ext_transport_calendar_id", "customs_value_currency", "quotation_no", "customer_tax_usage_type",
        "invoice_reason_id", "delivery_reason_id", "service_code", "component_a", "business_transaction_id"])
        
    retrieve_from_s3_customer_order_cft = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/customer_order_cft/",
        s3 = context.resources.s3_client,
        columns_to_drop=["cf_enter_by_date", "cf_attn", "cf_lcl_project_note2", 
                         "cf_lcl_project_note3", "cf_lcl_end_item", "cf_lcl_next_invc_date"])
        
    retrieve_from_s3_staged_billing_template_tab = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/staged_billing_template_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[])
        
    merged_df = pd.merge(
        retrieve_from_s3_customer_order_tab, 
        retrieve_from_s3_customer_order_cft,
        on="rowkey", how="left", 
        suffixes=("_customer_order_tab", "_customer_order_cft"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_staged_billing_template_tab,
        on="order_no", how="left", 
        suffixes=("", "_staged_billing_template_tab"))

    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df

@asset(
    group_name="Customer_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_cust_ord_price_hist_tab",
          "ifs_cust_order_line_address_tab",
          "ifs_cust_order_line_discount_tab",
          "ifs_customer_order_line_cft",
          "ifs_customer_order_line_tab",
          "ifs_order_line_staged_billing_cft",
          "ifs_order_line_staged_billing_tab"]
)
def customer_order_lines(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_customer_order_line_tab = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/customer_order_line_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
        "delivery_type", "supply_site_due_date", "dop_connection", "route_id", "forward_agent_id",
        "obsolete_column_sup_sm_object", "calc_char_price", "char_price", "dock_code", "sub_dock_code",
        "configured_line_price_id", "latest_release_date", "job_id", "cust_warranty_id", "condition_code",
        "originating_rel_no", "originating_line_item_no", "supply_site", "input_qty", "input_unit_meas",
        "input_conv_factor", "input_variable_values", "manufacturing_department", "delivery_sequence",
        "tax_id_validated_date", "customs_value", "classification_standard", "classification_part_no",
        "classification_unit_meas", "zone_id", "freight_map_id", "freight_price_list_no", "tax_class_id",
        "ext_transport_calendar_id", "qty_per_assembly", "new_comp_after_delivery", "packing_instruction_id",
        "originating_co_lang_code", "end_customer_id", "free_of_charge_tax_basis", "tax_calc_structure_id",
        "customer_tax_usage_type", "supply_site_part_no", "acquisition_origin", "statistical_code",
        "acquisition_reason_id", "equipment_object_seq", "hsn_sac_code", "br_unit_price"])
        
    retrieve_from_s3_customer_order_line_cft = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/customer_order_line_cft/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
        "cf_comm_percent", "cf_nan_emr_lineitemgid", "cf_nan_emr_ordgid", "cf_nan_emr_rev_gid",
        "cf_nan_product_family", "cf_bo_pkg_group", "cf_nan_estore_line_instr"])
        
    retrieve_from_s3_cust_ord_price_hist_tab = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/cust_ord_price_hist_tab/",
        s3 = context.resources.s3_client)
    
    retrieve_from_s3_order_line_staged_billing_tab = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/order_line_staged_billing_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=["milestone_id"])
        
    retrieve_from_s3_order_line_staged_billing_cft = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/order_line_staged_billing_cft/",
        s3 = context.resources.s3_client)
    
    retrieve_from_s3_cust_order_line_discount_tab = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/cust_order_line_discount_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=["discount_amount"])
        
    retrieve_from_s3_cust_order_line_address_tab = generic_extraction_and_cleaning(
        context = context, 
        key = "ifs/cust_order_line_address_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=["addr_6", "vat_no", "vat_free_vat_code", "address3", "address4", "address5", "address6"])

    merged_df = pd.merge(
        retrieve_from_s3_customer_order_line_tab, 
        retrieve_from_s3_customer_order_line_cft,
        on="rowkey", how="left", 
        suffixes=("_customer_order_line_tab", "_customer_order_line_cft"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_cust_ord_price_hist_tab,
        on=["order_no", "line_no", "rel_no"], how="left", 
        suffixes=("", "_cust_ord_price_hist_tab"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_order_line_staged_billing_tab,
        on=["order_no", "line_no", "rel_no"], how="left", 
        suffixes=("", "_order_line_staged_billing_tab"))    

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_order_line_staged_billing_cft,
        left_on="rowkey_order_line_staged_billing_tab", right_on="rowkey", how="left", 
        suffixes=("", "_order_line_staged_billing_cft"))  

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_cust_order_line_discount_tab,
        on=["order_no", "line_no", "rel_no"], how="left", 
        suffixes=("", "_cust_order_line_discount_tab"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_cust_order_line_address_tab,
        on=["order_no", "line_no", "rel_no"], how="left", 
        suffixes=("", "_cust_order_line_address_tab"))
    
    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df

@asset(
    group_name="Customer_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_cust_payment_mvt"]
)
def customer_payments(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/cust_payment_mvt/",
                                           columns_to_drop=["bank_fee2_curr_amount", "bank_fee2_dom_amount", "bank_fee2_parallel_amount"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Customer_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_cust_ord_invo_stat_tab"]
)
def customer_order_invoice_line_bridge(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/cust_ord_invo_stat_tab/",
                                           columns_to_drop=
                                           ["customer_price_group", "customer_price_grp_desc", "branch", 
                                            "rebate_assortment_id","rebate_assort_node_id", "condition_code", 
                                            "condition_code_description", "rowkey"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df
@asset(
    group_name="Customer_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_customer_order_pick_list_tab"]
)
def pick_lists(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/customer_order_pick_list_tab/",
                                           columns_to_drop=["sel_priority", "sel_shipment_id", "sel_consol_shipment_id", 
                                                            "sel_ship_date", "sel_shipment_location", "storage_zone_id"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df