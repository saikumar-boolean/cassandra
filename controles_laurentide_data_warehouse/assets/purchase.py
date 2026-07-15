from dagster import asset, AssetExecutionContext, EnvVar, AutomationCondition, AssetSelection
import pandas as pd
from .utils import generic_extraction_and_cleaning, rename_columns, add_shift_cron_time, ensure_snowflake_schema_matches_df

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()
IT_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=3, minutes_to_add=15)
PUT_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=3, minutes_to_add=30)
PAT_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=3, minutes_to_add=45)

@asset(
    group_name="Purchase",
    io_manager_key="snowflake_io_manager_test",
    required_resource_keys={"s3_client", "snowflake_resource_test"},
    deps=["ifs_inventory_transaction_hist_tab",
          "ifs_mpccom_accounting_tab"],
    automation_condition=AutomationCondition.on_cron(
        IT_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def inventory_transaction(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_inventory_transaction_hist_tab = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/inventory_transaction_hist_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False,
    )
    keep_cols_inventory_transaction_hist_tab = ['transaction_id', 'accounting_id', 'part_no', 'contract', 'location_no', 
                                                'lot_batch_no', 'serial_no', 'waiv_dev_rej_no', 'eng_chg_level', 'activity_seq',
                                                'order_no', 'release_no', 'sequence_no', 'line_item_no', 'reject_code',
                                                'transaction_code', 'pre_accounting_id', 'date_created', 'date_time_created',
                                                'date_applied', 'direction', 'order_type', 'partstat_flag', 'qty_reversed',
                                                'quantity', 'source', 'userid', 'valuestat_flag', 'owning_vendor_no',
                                                'owning_customer_no', 'part_ownership', 'original_transaction_id',
                                                'location_group', 'previous_part_ownership', 'previous_owning_customer_no',
                                                'previous_owning_vendor_no', 'expiration_date', 'receipt_date',
                                                'pre_purchase_transaction_hist_tab_level_qty_in_stock', 'pre_purchase_transaction_hist_tab_level_qty_in_purchase_transaction_hist_tabit',
                                                'project_id', 'catch_direction', 'alt_source_ref1', 'alt_source_ref2',
                                                'alt_source_ref3', 'alt_source_ref4', 'alt_source_ref_type',
                                                'inventory_part_cost_level', 'inventory_valuation_method',
                                                'modify_date_applied_date', 'modify_date_applied_user',
                                                'transit_location_group', 'report_earned_value', 'rowversion', 'rowkey',
                                                'availability_control_id', 'handling_unit_id', 'source_ref1', 'source_ref2',
                                                'source_ref3', 'source_ref4', 'source_ref5', 'source_ref_type',
                                                'alt_source_ref5', 'move_dest_contract', 'move_dest_location_no']
    
    retrieve_from_s3_inventory_transaction_hist_tab = retrieve_from_s3_inventory_transaction_hist_tab[keep_cols_inventory_transaction_hist_tab]
    retrieve_from_s3_inventory_transaction_hist_tab = rename_columns(df=retrieve_from_s3_inventory_transaction_hist_tab, old_cols=keep_cols_inventory_transaction_hist_tab,
                                                           new_cols=['TRANSACTION_ID', 'ACCOUNTING_ID', 'PART_NO', 'CONTRACT', 
                                                                     'LOCATION_NO', 'LOT_BATCH_NO', 'SERIAL_NO', 'WAIV_DEV_REJ_NO', 
                                                                     'ENG_CHG_LEVEL', 'ACTIVITY_SEQ', 'ORDER_NO', 'RELEASE_NO', 'SEQUENCE_NO', 
                                                                     'LINE_ITEM_NO', 'REJECT_CODE', 'TRANSACTION_CODE', 'PRE_ACCOUNTING_ID', 
                                                                     'DATE_CREATED', 'DATE_TIME_CREATED', 'DATE_APPLIED', 'DIRECTION', 'ORDER_TYPE', 
                                                                     'PARTSTAT_FLAG', 'QTY_REVERSED', 'QUANTITY', 'SOURCE', 'USERID', 'VALUESTAT_FLAG', 
                                                                     'OWNING_VENDOR_NO', 'OWNING_CUSTOMER_NO', 'PART_OWNERSHIP', 'ORIGINAL_purchase_transaction_hist_tabACTION_ID', 
                                                                     'LOCATION_GROUP', 'PREVIOUS_PART_OWNERSHIP', 'PREVIOUS_OWNING_CUSTOMER_NO', 
                                                                     'PREVIOUS_OWNING_VENDOR_NO', 'EXPIRATION_DATE', 'RECEIPT_DATE', 'PRE_purchase_transaction_hist_tab_LEVEL_QTY_IN_STOCK', 
                                                                     'PRE_purchase_transaction_hist_tab_LEVEL_QTY_IN_purchase_transaction_hist_tabIT', 'PROJECT_ID', 'CATCH_DIRECTION', 'ALT_SOURCE_REF1', 'ALT_SOURCE_REF2', 
                                                                     'ALT_SOURCE_REF3', 'ALT_SOURCE_REF4', 'ALT_SOURCE_REF_TYPE', 'INVENTORY_PART_COST_LEVEL', 
                                                                     'INVENTORY_VALUATION_METHOD', 'MODIFY_DATE_APPLIED_DATE', 'MODIFY_DATE_APPLIED_USER', 
                                                                     'TRANSIT_LOCATION_GROUP', 'REPORT_EARNED_VALUE', 'ROWVERSION', 'ROWKEY', 'AVAILABILITY_CONTROL_ID', 
                                                                     'HANDLING_UNIT_ID', 'ORDER_NO_REF', 'RELEASE_NO_REF', 'SEQUENCE_NO_REF', 'LINE_ITEM_NO_REF', 
                                                                     'SOURCE_REF5', 'ORDER_TYPE_REF', 'ALT_SOURCE_REF5', 'MOVE_DEST_CONTRACT', 'MOVE_DEST_LOCATION_NO'])

    retrieve_from_s3_mpccom_accounting_tab = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/mpccom_accounting_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False
    )

    keep_cols_mpccom_accounting_tab = ['accounting_id', 'seq', 'company', 'account_no', 'codeno_b', 'codeno_c', 'codeno_d', 
                                       'codeno_e', 'codeno_f', 'codeno_g', 'codeno_h', 'voucher_no', 'str_code', 'event_code',
                                       'voucher_type', 'debit_credit', 'value', 'status_code', 'booking_source', 'curr_amount',
                                       'date_applied', 'currency_code', 'currency_rate', 'activity_seq', 'accounting_year',
                                       'accounting_period', 'contract', 'inventory_value_status', 'date_of_origin',
                                       'original_accounting_id', 'original_seq', 'userid', 'bucket_posting_group_id',
                                       'rowversion', 'rowkey', 'trans_reval_event_id']
    

    retrieve_from_s3_mpccom_accounting_tab = retrieve_from_s3_mpccom_accounting_tab[keep_cols_mpccom_accounting_tab]


    retrieve_from_s3_mpccom_accounting_tab = rename_columns(df=retrieve_from_s3_mpccom_accounting_tab, old_cols=keep_cols_mpccom_accounting_tab, 
                   new_cols=['ACCOUNTING_ID', 'SEQ', 'COMPANY', 'ACCOUNT_NO', 'CODENO_B', 'CODENO_C', 'CODENO_D', 
                             'CODENO_E', 'CODENO_F', 'CODENO_G', 'CODENO_H', 'VOUCHER_NO', 'STR_CODE', 'EVENT_CODE', 
                             'VOUCHER_TYPE', 'DEBIT_OR_CREDIT', 'COST', 'STATUS_CODE', 'BOOKING_SOURCE', 'CURR_AMOUNT', 
                             'TRANSACTION_DATE', 'CURRENCY_CODE', 'CURRENCY_RATE', 'ACTIVITY_SEQ', 'ACCOUNTING_YEAR', 
                             'ACCOUNTING_PERIOD', 'CONTRACT', 'INVENTORY_VALUE_STATUS', 'DATE_OF_ORIGIN', 
                             'ORIGINAL_ACCOUNTING_ID', 'ORIGINAL_SEQ', 'USERID', 'BUCKET_POSTING_GROUP_ID', 'ROWVERSION', 
                             'ROWKEY', 'TRANS_REVAL_EVENT_ID'])
    
    retrieve_from_s3_mpccom_accounting_tab.loc[retrieve_from_s3_mpccom_accounting_tab["DEBIT_OR_CREDIT"] == "C", "COST"] = -retrieve_from_s3_mpccom_accounting_tab.loc[retrieve_from_s3_mpccom_accounting_tab["DEBIT_OR_CREDIT"] == "C", "COST"]

    merged_df = pd.merge(
        retrieve_from_s3_mpccom_accounting_tab,
        retrieve_from_s3_inventory_transaction_hist_tab,
        on="ACCOUNTING_ID", how="left",
        suffixes=("_mpccom_accounting_tab", "_inventory_transaction_hist_tab"))

    
    ensure_snowflake_schema_matches_df(context, merged_df, context.resources.snowflake_resource_test)

    return merged_df

@asset(
    group_name="Purchase",
    io_manager_key="snowflake_io_manager_test",
    required_resource_keys={"s3_client", "snowflake_resource_test"},
    deps=["ifs_purchase_transaction_hist_tab",
          "ifs_mpccom_accounting_tab",
          "ifs_purchase_order_line_tab"],
    automation_condition=AutomationCondition.on_cron(
        PUT_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def purchase_transaction(context: AssetExecutionContext) -> pd.DataFrame:

    retrieve_from_s3_purchase_transaction_hist_tab = generic_extraction_and_cleaning(
        context=context,
        key="ifs/purchase_transaction_hist_tab/",
        s3=context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False,
    )

    keep_cols_purchase_transaction_hist_tab = [
        'transaction_id','accounting_id','part_no','contract','order_no',
        'release_no','sequence_no','line_item_no','reject_code','transaction_code',
        'voucher_no','pre_accounting_id','voucher_type','dated','date_applied',
        'direction','qty_reversed','quantity','userid','status_code',
        'original_transaction_id','modify_date_applied_date',
        'modify_date_applied_user','order_type','rowversion','rowkey'
    ]

    retrieve_from_s3_purchase_transaction_hist_tab = retrieve_from_s3_purchase_transaction_hist_tab[keep_cols_purchase_transaction_hist_tab]

    retrieve_from_s3_purchase_transaction_hist_tab = rename_columns(
        df=retrieve_from_s3_purchase_transaction_hist_tab,
        old_cols=keep_cols_purchase_transaction_hist_tab,
        new_cols=[
            'TRANSACTION_ID','ACCOUNTING_ID','PART_NO','CONTRACT','ORDER_NO',
            'RELEASE_NO','SEQUENCE_NO','LINE_ITEM_NO','REJECT_CODE','TRANSACTION_CODE',
            'VOUCHER_NO','PRE_ACCOUNTING_ID','VOUCHER_TYPE','DATED','DATE_APPLIED',
            'DIRECTION','QTY_REVERSED','QUANTITY','USERID','STATUS_CODE',
            'ORIGINAL_purchase_transaction_hist_tabACTION_ID','MODIFY_DATE_APPLIED_DATE',
            'MODIFY_DATE_APPLIED_USER','ORDER_TYPE','ROWVERSION','ROWKEY'
        ]
    )

    retrieve_from_s3_mpccom_accounting_tab = generic_extraction_and_cleaning(
        context=context,
        key="ifs/mpccom_accounting_tab/",
        s3=context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False
    )

    keep_cols_mpccom_accounting_tab = [
        'accounting_id','seq','company','account_no','codeno_b','codeno_c','codeno_d',
        'codeno_e','codeno_f','codeno_g','codeno_h','voucher_no','str_code','event_code',
        'voucher_type','debit_credit','value','status_code','booking_source','curr_amount',
        'date_applied','currency_code','currency_rate','activity_seq','accounting_year',
        'accounting_period','contract','inventory_value_status','date_of_origin',
        'original_accounting_id','original_seq','userid','bucket_posting_group_id',
        'rowversion','rowkey','trans_reval_event_id'
    ]

    retrieve_from_s3_mpccom_accounting_tab = retrieve_from_s3_mpccom_accounting_tab[keep_cols_mpccom_accounting_tab]

    retrieve_from_s3_mpccom_accounting_tab = rename_columns(
        df=retrieve_from_s3_mpccom_accounting_tab,
        old_cols=keep_cols_mpccom_accounting_tab,
        new_cols=[
            'ACCOUNTING_ID','SEQ','COMPANY','ACCOUNT_NO','CODENO_B','CODENO_C','CODENO_D',
            'CODENO_E','CODENO_F','CODENO_G','CODENO_H','VOUCHER_NO','STR_CODE','EVENT_CODE',
            'VOUCHER_TYPE','DEBIT_OR_CREDIT','COST','STATUS_CODE','BOOKING_SOURCE','CURR_AMOUNT',
            'TRANSACTION_DATE','CURRENCY_CODE','CURRENCY_RATE','ACTIVITY_SEQ','ACCOUNTING_YEAR',
            'ACCOUNTING_PERIOD','CONTRACT','INVENTORY_VALUE_STATUS','DATE_OF_ORIGIN',
            'ORIGINAL_ACCOUNTING_ID','ORIGINAL_SEQ','USERID','BUCKET_POSTING_GROUP_ID',
            'ROWVERSION','ROWKEY','TRANS_REVAL_EVENT_ID'
        ]
    )

    retrieve_from_s3_mpccom_accounting_tab.loc[retrieve_from_s3_mpccom_accounting_tab["DEBIT_OR_CREDIT"] == "C", "COST"] = -retrieve_from_s3_mpccom_accounting_tab.loc[retrieve_from_s3_mpccom_accounting_tab["DEBIT_OR_CREDIT"] == "C", "COST"]

    retrieve_from_s3_purchase_order_line_tab = generic_extraction_and_cleaning(
        context=context,
        key="ifs/purchase_order_line_tab/",
        s3=context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False,
    )

    keep_cols_purchase_order_line_tab = [
        'order_no','line_no','release_no','demand_order_no',
        'demand_release','demand_sequence_no','demand_code',
        'project_id','activity_seq'
    ]

    retrieve_from_s3_purchase_order_line_tab = retrieve_from_s3_purchase_order_line_tab[keep_cols_purchase_order_line_tab]

    retrieve_from_s3_purchase_order_line_tab = rename_columns(
        df=retrieve_from_s3_purchase_order_line_tab,
        old_cols=keep_cols_purchase_order_line_tab,
        new_cols=[
            'ORDER_NO','LINE_NO','RELEASE_NO','DEMAND_ORDER_NO',
            'DEMAND_RELEASE','DEMAND_SEQUENCE_NO','DEMAND_CODE',
            'PROJECT_ID','ACTIVITY_SEQ'
        ]
    )

    merged_df = pd.merge(
        retrieve_from_s3_purchase_order_line_tab,
        retrieve_from_s3_purchase_transaction_hist_tab,
        left_on=["ORDER_NO","LINE_NO","RELEASE_NO"],
        right_on=["ORDER_NO","RELEASE_NO","SEQUENCE_NO"],
        how="left",
        suffixes=("_purchase_order_line_tab","_purchase_transaction_hist_tab")
    )

    merged_df = pd.merge(
        retrieve_from_s3_mpccom_accounting_tab,
        merged_df,
        on="ACCOUNTING_ID",
        how="left",
        suffixes=("_mpccom_accounting_tab","")
    )

    merged_df = merged_df[
        (merged_df["STR_CODE"] == "M92") &
        (merged_df["ACCOUNT_NO"].isin(["0700510","0701260"]))
    ]

    context.log.info(f"COLS: {merged_df.columns.tolist()}")

    result = merged_df.groupby([
        "DEMAND_CODE",
        "ORDER_NO",
        "RELEASE_NO_purchase_transaction_hist_tab",
        "SEQUENCE_NO",
        "LINE_ITEM_NO",
        "DEMAND_ORDER_NO",
        "DEMAND_RELEASE",
        "DEMAND_SEQUENCE_NO",
        "PROJECT_ID",
        "ACTIVITY_SEQ_mpccom_accounting_tab",
        "TRANSACTION_DATE",
        "PART_NO",
        "COMPANY"
    ], dropna=False)["COST"].sum().reset_index()

    ensure_snowflake_schema_matches_df(
        context,
        result,
        context.resources.snowflake_resource_test
    )

    return result

@asset(
    group_name="Purchase",
    io_manager_key="snowflake_io_manager_test",
    required_resource_keys={"s3_client", "snowflake_resource_test"},
    deps=["ifs_inv_accounting_row_tab",
          "ifs_purchase_order_line_tab",
          "ifs_invoice_tab"],
    automation_condition=AutomationCondition.on_cron(
        PAT_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def payable_transaction(context: AssetExecutionContext) -> pd.DataFrame:

    retrieve_from_s3_invoice_tab = generic_extraction_and_cleaning(
        context=context,
        key="ifs/invoice_tab/",
        s3=context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False,
    )

    keep_cols_invoice_tab = [
        "invoice_id", "invoice_no", "company"
    ]

    retrieve_from_s3_invoice_tab = retrieve_from_s3_invoice_tab[keep_cols_invoice_tab]

    retrieve_from_s3_invoice_tab = rename_columns(
        df=retrieve_from_s3_invoice_tab,
        old_cols=keep_cols_invoice_tab,
        new_cols=[
            "INVOICE_ID", "INVOICE_NO", "COMPANY"
        ]
    )

    retrieve_from_s3_inv_accounting_row_tab = generic_extraction_and_cleaning(
        context=context,
        key="ifs/inv_accounting_row_tab/",
        s3=context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False,
    )

    keep_cols_inv_accounting_row_tab = [
        "invoice_id","row_id","order_no","reference",
        "voucher_date","company","posting_type","code_a","dom_amount", "rowtype"
    ]

    retrieve_from_s3_inv_accounting_row_tab = retrieve_from_s3_inv_accounting_row_tab[keep_cols_inv_accounting_row_tab]

    retrieve_from_s3_inv_accounting_row_tab = rename_columns(
        df=retrieve_from_s3_inv_accounting_row_tab,
        old_cols=keep_cols_inv_accounting_row_tab,
        new_cols=[
            "INVOICE_ID","ROW_ID","ORDER_NO","REFERENCE",
            "TRANSACTION_DATE","COMPANY","POSTING_TYPE","CODE_A","COST", "ROWTYPE"
        ]
    )

    retrieve_from_s3_inv_accounting_row_tab = retrieve_from_s3_inv_accounting_row_tab[retrieve_from_s3_inv_accounting_row_tab["ROWTYPE"].str.endswith("ManSuppInvoicePostings", na=False)]


    ref_split = retrieve_from_s3_inv_accounting_row_tab["REFERENCE"].str.split("^", expand=True)

    retrieve_from_s3_inv_accounting_row_tab["RELEASE_NO"] = ref_split[1]
    retrieve_from_s3_inv_accounting_row_tab["SEQUENCE_NO"] = ref_split[2]
    retrieve_from_s3_inv_accounting_row_tab["LINE_ITEM_NO"] = ref_split[3]

    retrieve_from_s3_purchase_order_line_tab = generic_extraction_and_cleaning(
        context=context,
        key="ifs/purchase_order_line_tab/",
        s3=context.resources.s3_client,
        columns_to_drop=[], dynamic_drop=False,
    )

    keep_cols_purchase_order_line_tab = [
        "order_no","line_no","release_no","demand_code",
        "demand_order_no","demand_release","demand_sequence_no",
        "project_id","activity_seq","part_no"
    ]

    retrieve_from_s3_purchase_order_line_tab = retrieve_from_s3_purchase_order_line_tab[keep_cols_purchase_order_line_tab]

    retrieve_from_s3_purchase_order_line_tab = rename_columns(
        df=retrieve_from_s3_purchase_order_line_tab,
        old_cols=keep_cols_purchase_order_line_tab,
        new_cols=[
            "ORDER_NO","LINE_NO","RELEASE_NO","DEMAND_CODE",
            "DEMAND_ORDER_NO","DEMAND_RELEASE","DEMAND_SEQUENCE_NO",
            "PROJECT_ID","ACTIVITY_SEQ","PART_NO"
        ]
    )

    merged_df = pd.merge(
        retrieve_from_s3_inv_accounting_row_tab,
        retrieve_from_s3_purchase_order_line_tab,
        left_on=["ORDER_NO","RELEASE_NO","SEQUENCE_NO"],
        right_on=["ORDER_NO","LINE_NO","RELEASE_NO"],
        how="left", 
        suffixes=("_purchase_transaction_hist_tab", "_purchase_order_line_tab")
    )

    merged_df = pd.merge(
        merged_df,
        retrieve_from_s3_invoice_tab,
        on=["COMPANY", "INVOICE_ID"],
        how="left", 
        suffixes=("", "_invoice_tab,")
    )

    merged_df = merged_df[
        (merged_df["POSTING_TYPE"].isin(["M92","M93","M19","M20"])) &
        (merged_df["CODE_A"].isin(["0700510","0701260"]))
    ]

    context.log.info(f"COLS: {merged_df.columns.tolist()}")

    result = merged_df.groupby([
        "DEMAND_CODE",
        "INVOICE_ID",
        "INVOICE_NO",
        "ROW_ID",
        "ORDER_NO",
        "RELEASE_NO_purchase_transaction_hist_tab",
        "SEQUENCE_NO",
        "LINE_ITEM_NO",
        "DEMAND_ORDER_NO",
        "DEMAND_RELEASE",
        "DEMAND_SEQUENCE_NO",
        "PROJECT_ID",
        "ACTIVITY_SEQ",
        "PART_NO",
        "TRANSACTION_DATE",
        "COMPANY"
    ], dropna=False)["COST"].sum().reset_index()

    ensure_snowflake_schema_matches_df(
        context,
        result,
        context.resources.snowflake_resource_test
    )

    return result
