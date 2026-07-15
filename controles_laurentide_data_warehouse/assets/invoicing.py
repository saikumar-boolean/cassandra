import pandas as pd
from dagster import asset, AssetExecutionContext, AutomationCondition, EnvVar, AssetSelection
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df, add_shift_cron_time, rename_columns

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()
NEW_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=7, minutes_to_add=15)

@asset(
    group_name="Invoicing",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_advance_inv_reference_tab",
          "ifs_invoice_tab",
          "ifs_invoice_item_tab"],
    automation_condition=AutomationCondition.on_cron(
        NEW_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def invoice_lines(context: AssetExecutionContext) -> pd.DataFrame:
    
    retrieve_from_s3_invoice_item_tab = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/invoice_item_tab/",
                                           columns_to_drop=['net_amount', 'vat_amount', 'deliv_type_id', 'order_no', 'order_line_no', 'order_part_no', 
                                                            'order_price', 'order_qty', 'c14', 'c15', 'c17', 'c18', 'c20', 'd1', 'd2', 'd3', 'd4', 
                                                            'reason_code_id', 'copy_tax_from_id', 'copy_tax_from_item', 'irs1099_type_id', 
                                                            'man_tax_liability_date', 'tax_class_id', 'n16', 'net_parallel_amount', 'parallel_curr_rate', 
                                                            'parallel_div_factor', 'copied_fee_rounding', 'po_sequence_number', 'invoicing_advice_id', 
                                                            'sup_add_info', 'tax_calc_structure_id', 'correction_reason_id', 'correction_reason', 
                                                            'customs_declaration_no', 'c21', 'n18', 'n19', 'customer_tax_usage_type', 'tax_book_id', 
                                                            'tax_series_id', 'tax_series_no', 'ip10_tax_book_id', 'ip10_tax_series_id', 'ip10_tax_series_no', 
                                                            'business_operation', 'acquisition_origin', 'statistical_code', 'prepay_tax_curr_amount', 
                                                            'prepay_tax_dom_amount', 'prepay_tax_base_curr_amt', 'prepay_tax_base_dom_amt', 
                                                            'prepay_tax_document_id', 'document_type_code', 'manual_tax_base_curr_amt', 'hsn_sac_code'])

    keep_cols_invoice_item_tab = ['company', 'identity', 'party_type', 
                                  'invoice_id', 'item_id', 'item_data', 'creator', 
                                  'net_curr_code', 'net_rate', 'net_curr_amount', 'net_dom_amount', 
                                  'vat_curr_amount', 'vat_dom_amount', 'reference', 'vat_code', 'c1', 
                                  'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11', 'c12', 
                                  'c13', 'c16', 'c19', 'n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'n7', 'n8', 
                                  'n9', 'n10', 'n11', 'n12', 'n13', 'part_no', 'part_description', 
                                  'unit_of_measure', 'price_unit_of_measure', 'po_ref_number', 'withheld_tax_curr_amount', 
                                  'withheld_tax_dom_amount', 'receipt_ref', 'matching_result', 'charge_line', 'series_reference', 
                                  'number_reference', 'debit_invoice_id', 'prel_update_allowed', 'allocation_id', 'sup_part_no', 
                                  'sup_part_desc', 'n14', 'n15', 'vat_parallel_amount', 'withheld_tax_parallel_amt', 'voucher_text', 
                                  'actual_net_curr_amount', 'actual_net_dom_amount', 'actual_net_parallel_amount', 
                                  'non_ded_tax_parallel_amt', 'po_line_number', 'po_release_number', 'po_receipt_number', 
                                  'inv_net_unit_price', 'n17', 'c22', 'delivery_country', 'n20', 'n21', 'n22']

    retrieve_from_s3_invoice_item_tab = retrieve_from_s3_invoice_item_tab[keep_cols_invoice_item_tab]
    retrieve_from_s3_invoice_item_tab = rename_columns(df=retrieve_from_s3_invoice_item_tab, old_cols=keep_cols_invoice_item_tab,
                   new_cols=['company', 'customer_no', 'party_type', 'invoice_id', 'item_id', 'item_data', 'creator', 
                             'net_curr_code', 'net_rate', 'selling_total_amount', 'selling_amount', 'vat_curr_amount', 
                             'vat_dom_amount', 'reference', 'vat_code', 'order_no', 'line_no', 'release_no', 'pos', 
                             'catalog_no', 'description', 'sale_um', 'price_um', 'customer_po_no', 'contract', 'charge_group', 
                             'configuration_id', 'delivery_customer', 'sales_part_rebate_group', 'ship_address_no', 'line_item_no', 
                             'invoiced_qty', 'price_conv', 'sale_unit_preice', 'discount', 'order_discount', 'charge_seq_no', 
                             'stage', 'rma_no', 'rma_line_no', 'rma_charge_no', 'additional_discount', 'charge_percent', 'part_no', 
                             'part_description', 'unit_of_measure', 'price_unit_of_measure', 'po_ref_number', 'withheld_tax_curr_amount', 
                             'withheld_tax_dom_amount', 'receipt_ref', 'matching_result', 'charge_line', 'series_reference', 
                             'number_reference', 'debit_invoice_id', 'prel_update_allowed', 'allocation_id', 'sup_part_no', 
                             'sup_part_desc', 'charge_percent_basis', 'unit_price_incl_tax', 'vat_parallel_amount', 
                             'withheld_tax_parallel_amt', 'voucher_text', 'actual_net_curr_amount', 'actual_net_dom_amount', 
                             'actual_net_parallel_amount', 'non_ded_tax_parallel_amt', 'po_line_number', 'po_release_number', 
                             'po_receipt_number', 'inv_net_unit_price', 'original_invoiced_qty', 'free_of_charge', 'delivery_country', 
                             'original_discount', 'original_add_discount', 'original_order_discount'])
    
    retrieve_from_s3_invoice_item_tab.insert(
        retrieve_from_s3_invoice_item_tab.columns.get_loc('net_curr_code') + 1,
        'vat_rate',
        retrieve_from_s3_invoice_item_tab['net_rate']
    )

    retrieve_from_s3_invoice_item_tab.insert(
        retrieve_from_s3_invoice_item_tab.columns.get_loc('selling_amount') + 1,
        'gross_curr_amount',
        retrieve_from_s3_invoice_item_tab['selling_total_amount'] + retrieve_from_s3_invoice_item_tab['vat_curr_amount']
    )
    retrieve_from_s3_invoice_tab = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/invoice_tab/",
                                           columns_to_drop=['national_bank_code', 'prelim_code', 'pre_acc_code_string', 
                                                            'delivery_party', 'withheld_voucher_no', 'withheld_voucher_type', 
                                                            'withheld_accounting_year', 'code_b', 'code_f', 'code_h', 'code_i', 
                                                            'c9', 'c10', 'n3', 'n4', 'd4', 'chk_handling_code', 'multi_invoice_voucher', 
                                                            'voucher_final_post', 'voucher_vat_only', 'automatic_invoice', 'transfer_status', 
                                                            'transfer_error', 'deduction_group', 'branch', 'receipt_ref', 'batch_id', 'self_billing_ref', 
                                                            'bank_account', 'nature_of_business', 'tax_curr_type', 'sub_con_no', 'js_virtual_invoice_no', 
                                                            'msg_sequence_no', 'msg_version_no', 'n5', 'media_code', 'send_error', 'attachments', 
                                                            'send_flag_attr', 'send_msg_id', 'send_error_step', 'tax_id_type', 'tax_id_number', 
                                                            'net_parallel_amount', 'parallel_curr_rate', 'parallel_div_factor', 'one_time_address_id', 
                                                            'additional_reference', 'invoicing_advice_id', 'c15', 'c16', 'correction_reason_id', 
                                                            'correction_reason', 'customs_declaration_date', 'invoice_reason_id', 'credit_invoice_id', 
                                                            'service_code', 'xml_file_name_suffix', 'component_a', 'component_b', 
                                                            'component_c', 'prepayment_type_code', 
                                                            'prepay_adv_inv_id', 'prepay_ledger_item_series', 'prepay_ledger_item_id', 
                                                            'prepay_ledger_item_version', 'internal_series_id', 'internal_invoice_no', 'document_type_code', 
                                                            'related_uuid_number', 'irn', 'qr_data', 
                                                            'invoice_tax_id_number', 'delivery_tax_id_number', 'delivery_inv_address_id'])

    keep_cols_invoice_tab = ['invoice_id', 'series_id', 'invoice_no', 'invoice_date', 'due_date', 'invoice_type', 'pay_term_id', 
                                  'aff_base_ledg_post', 'aff_line_post', 'delivery_date', 'arrival_date', 'series_reference', 
                                  'number_reference', 'delivery_identity', 'delivery_address_id', 'invoice_address_id', 
                                  'creators_reference', 'creation_date', 'curr_rate', 'div_factor', 'jour_no_print', 'jour_no_voucher', 
                                  'payment_address_id', 'way_pay_id', 'canceled_time', 'voucher_no_ref', 'voucher_type_ref', 
                                  'voucher_date_ref', 'accounting_year_ref', 'accounting_period_ref', 'acc_year_invoice_date', 
                                  'acc_period_invoice_date', 'currency', 'post_error', 'error_text', 'tax_curr_rate', 'voucher_text', 
                                  'tax_invoice_series_id', 'tax_invoice_number', 'tax_invoice_date', 'correction', 'js_invoice_state', 
                                  'price_adjustment', 'original_result_key', 'debit_invoice_id', 'correction_invoice_id', 'send_status', 
                                  'send_time', 'supply_country', 'tax_liability', 'project_id', 'load_type', 'inv_actual_net_curr_amt', 
                                  'inv_vat_curr_amt', 'non_deduct_tax_dom_amount', 
                                  'actual_net_parallel_amount', 'non_ded_tax_parallel_amt', 'bi_timestamp', 'invoice_text_id', 
                                  'invoice_text', 'use_proj_address_for_tax', 'latest_result_key', 'actual_net_updated', 'posted_date', 
                                  'tax_withh_curr_rate', 'supply_delivery_address_id', 'rowstate', 'creator', 'rowkey', 'party_type', 'company']
    retrieve_from_s3_invoice_tab = retrieve_from_s3_invoice_tab[keep_cols_invoice_tab]
    retrieve_from_s3_invoice_tab = rename_columns(df=retrieve_from_s3_invoice_tab, old_cols=keep_cols_invoice_tab,
                   new_cols=['invoice_id', 'series_id', 'invoice_no', 'invoice_date', 'due_date', 'invoice_type', 'pay_term_id', 'aff_base_ledg_post', 
                             'aff_line_post', 'delivery_date', 'arrival_date', 'series_reference', 'number_reference', 'delivery_identity',
                             'delivery_address_id', 'invoice_address_id', 'creators_reference', 'creation_date', 'currency_rate', 
                             'div_factor', 'jour_no_print', 'jour_no_voucher', 'payment_address_id', 'way_pay_id', 'canceled_time', 
                             'voucher_no_ref', 'voucher_type_ref', 'voucher_date_ref', 'accounting_year_ref', 'accounting_period_ref', 
                             'acc_year_invoice_date', 'acc_period_invoice_date', 'currency', 'post_error', 'error_text', 'tax_curr_rate', 
                             'voucher_text', 'tax_invoice_series_id', 'tax_invoice_number', 'tax_invoice_date', 'correction', 
                             'js_invoice_state', 'price_adjustment', 'original_result_key', 'debit_invoice_id', 'correction_invoice_id', 
                             'send_status', 'send_time', 'supply_country', 'tax_liability', 'project_id', 'load_type', 
                             'inv_actual_net_curr_amt', 'inv_vat_curr_amt', 
                             'non_deduct_tax_dom_amount', 'actual_net_parallel_amount', 'non_ded_tax_parallel_amt', 'bi_timestamp', 
                             'invoice_text_id', 'invoice_text', 'use_proj_address_for_tax', 'latest_result_key', 'actual_net_updated', 
                             'posted_date', 'tax_withh_curr_rate', 'supply_delivery_address_id', 'rowstate', 'creator', 'rowkey', 'party_type', 'company'])
    
    retrieve_from_s3_advance_inv_reference_tab = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/advance_inv_reference_tab/",
                                           columns_to_drop=['offset_parallel_amount'])

    merged_df = pd.merge(
        retrieve_from_s3_invoice_item_tab, 
        retrieve_from_s3_invoice_tab,
        on=["invoice_id", "company"], how="left", 
        suffixes=("_invoice_item_tab", "_invoice_tab"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_advance_inv_reference_tab,
        on=["invoice_id", "company"], how="left", 
        suffixes=("_advance_inv_reference_tab", "_advance_inv_reference_tab,"))
    
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df