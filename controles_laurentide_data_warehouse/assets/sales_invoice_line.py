from dagster import asset, AssetExecutionContext, AutomationCondition, EnvVar, AssetSelection
from .utils import generic_extraction_and_cleaning, rename_columns, add_surrogate_key, ensure_snowflake_schema_matches_df, add_shift_cron_time
import pandas as pd

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()
NEW_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=6, minutes_to_add=0)

@asset(
    group_name="Sales_Invoice_Line",
    required_resource_keys={"s3_client", "snowflake_resource"},
    io_manager_key="snowflake_io_manager",
    tags={"heavy_asset": "true"},
    deps=["ifs_invoice_item_tab", 
          "ifs_invoice_tab"],
    automation_condition=AutomationCondition.on_cron(
        NEW_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def sales_invoice_line(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_invoice_item_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/invoice_item_tab/",
        columns_to_drop=[])

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
                   new_cols=['COMPANY', 'CUSTOMER_NO', 'PARTY_TYPE', 'INVOICE_ID', 'ITEM_ID', 'ITEM_DATA', 'CREATOR', 
                             'NET_CURR_CODE', 'NET_RATE', 'SELLING_TOTAL_AMOUNT', 'SELLING_AMOUNT', 'VAT_CURR_AMOUNT', 
                             'VAT_DOM_AMOUNT', 'REFERENCE', 'VAT_CODE', 'ORDER_NO', 'LINE_NO', 'RELEASE_NO', 'POS', 
                             'CATALOG_NO', 'DESCRIPTION', 'SALE_UM', 'PRICE_UM', 'CUSTOMER_PO_NO', 'CONTRACT', 'CHARGE_GROUP', 
                             'CONFIGURATION_ID', 'DELIVERY_CUSTOMER', 'SALES_PART_REBATE_GROUP', 'SHIP_ADDRESS_NO', 'LINE_ITEM_NO', 
                             'INVOICED_QTY', 'PRICE_CONV', 'SALE_UNIT_PREICE', 'DISCOUNT', 'ORDER_DISCOUNT', 'CHARGE_SEQ_NO', 
                             'STAGE', 'RMA_NO', 'RMA_LINE_NO', 'RMA_CHARGE_NO', 'ADDITIONAL_DISCOUNT', 'CHARGE_PERCENT', 'PART_NO', 
                             'PART_DESCRIPTION', 'UNIT_OF_MEASURE', 'PRICE_UNIT_OF_MEASURE', 'PO_REF_NUMBER', 'WITHHELD_TAX_CURR_AMOUNT', 
                             'WITHHELD_TAX_DOM_AMOUNT', 'RECEIPT_REF', 'MATCHING_RESULT', 'CHARGE_LINE', 'SERIES_REFERENCE', 
                             'NUMBER_REFERENCE', 'DEBIT_INVOICE_ID', 'PREL_UPDATE_ALLOWED', 'ALLOCATION_ID', 'SUP_PART_NO', 
                             'SUP_PART_DESC', 'CHARGE_PERCENT_BASIS', 'UNIT_PRICE_INCL_TAX', 'VAT_PARALLEL_AMOUNT', 
                             'WITHHELD_TAX_PARALLEL_AMT', 'VOUCHER_TEXT', 'ACTUAL_NET_CURR_AMOUNT', 'ACTUAL_NET_DOM_AMOUNT', 
                             'ACTUAL_NET_PARALLEL_AMOUNT', 'NON_DED_TAX_PARALLEL_AMT', 'PO_LINE_NUMBER', 'PO_RELEASE_NUMBER', 
                             'PO_RECEIPT_NUMBER', 'INV_NET_UNIT_PRICE', 'ORIGINAL_INVOICED_QTY', 'FREE_OF_CHARGE', 'DELIVERY_COUNTRY', 
                             'ORIGINAL_DISCOUNT', 'ORIGINAL_ADD_DISCOUNT', 'ORIGINAL_ORDER_DISCOUNT'])
    
    retrieve_from_s3_invoice_item_tab.insert(
        retrieve_from_s3_invoice_item_tab.columns.get_loc('NET_CURR_CODE') + 1,
        'VAT_RATE',
        retrieve_from_s3_invoice_item_tab['NET_RATE']
    )

    retrieve_from_s3_invoice_item_tab.insert(
        retrieve_from_s3_invoice_item_tab.columns.get_loc('SELLING_AMOUNT') + 1,
        'GROSS_CURR_AMOUNT',
        retrieve_from_s3_invoice_item_tab['SELLING_TOTAL_AMOUNT'] + retrieve_from_s3_invoice_item_tab['VAT_CURR_AMOUNT']
    )
    retrieve_from_s3_invoice_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/invoice_tab/",
        columns_to_drop=[])

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
                   new_cols=['INVOICE_ID', 'SERIES_ID', 'INVOICE_NO', 'INVOICE_DATE', 'DUE_DATE', 'INVOICE_TYPE', 'PAY_TERM_ID', 'AFF_BASE_LEDG_POST', 
                             'AFF_LINE_POST', 'DELIVERY_DATE', 'ARRIVAL_DATE', 'SERIES_REFERENCE', 'NUMBER_REFERENCE', 'DELIVERY_IDENTITY',
                             'DELIVERY_ADDRESS_ID', 'INVOICE_ADDRESS_ID', 'CREATORS_REFERENCE', 'CREATION_DATE', 'CURRENCY_RATE', 
                             'DIV_FACTOR', 'JOUR_NO_PRINT', 'JOUR_NO_VOUCHER', 'PAYMENT_ADDRESS_ID', 'WAY_PAY_ID', 'CANCELED_TIME', 
                             'VOUCHER_NO_REF', 'VOUCHER_TYPE_REF', 'VOUCHER_DATE_REF', 'ACCOUNTING_YEAR_REF', 'ACCOUNTING_PERIOD_REF', 
                             'ACC_YEAR_INVOICE_DATE', 'ACC_PERIOD_INVOICE_DATE', 'CURRENCY', 'POST_ERROR', 'ERROR_TEXT', 'TAX_CURR_RATE', 
                             'VOUCHER_TEXT', 'TAX_INVOICE_SERIES_ID', 'TAX_INVOICE_NUMBER', 'TAX_INVOICE_DATE', 'CORRECTION', 
                             'JS_INVOICE_STATE', 'PRICE_ADJUSTMENT', 'ORIGINAL_RESULT_KEY', 'DEBIT_INVOICE_ID', 'CORRECTION_INVOICE_ID', 
                             'SEND_STATUS', 'SEND_TIME', 'SUPPLY_COUNTRY', 'TAX_LIABILITY', 'PROJECT_ID', 'LOAD_TYPE', 
                             'INV_ACTUAL_NET_CURR_AMT', 'INV_VAT_CURR_AMT', 
                             'NON_DEDUCT_TAX_DOM_AMOUNT', 'ACTUAL_NET_PARALLEL_AMOUNT', 'NON_DED_TAX_PARALLEL_AMT', 'BI_TIMESTAMP', 
                             'INVOICE_TEXT_ID', 'INVOICE_TEXT', 'USE_PROJ_ADDRESS_FOR_TAX', 'LATEST_RESULT_KEY', 'ACTUAL_NET_UPDATED', 
                             'POSTED_DATE', 'TAX_WITHH_CURR_RATE', 'SUPPLY_DELIVERY_ADDRESS_ID', 'ROWSTATE', 'CREATOR', 'ROWKEY', 'PARTY_TYPE', 'COMPANY'])
    


    merged_df = pd.merge(
        retrieve_from_s3_invoice_tab, 
        retrieve_from_s3_invoice_item_tab,
        on=["INVOICE_ID", "COMPANY"], how="right", 
        suffixes=("_invoice_tab", "_invoice_item_tab"))
    
    context.log.info(f"DateType: {merged_df['INVOICE_DATE'].dtype}")

    merged_df = add_surrogate_key(df=merged_df, key_name='SK_SALES_INVOICE_LINE_ID', by=["INVOICE_ID", "ITEM_ID"])
    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df