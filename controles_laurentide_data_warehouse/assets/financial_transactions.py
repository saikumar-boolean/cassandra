import pandas as pd
from dagster import asset, AssetExecutionContext, AutomationCondition, EnvVar, AssetSelection
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df, add_shift_cron_time

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()

@asset(
    group_name="Financial_Transactions",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_mpccom_transaction_code_tab"]
)
def transactions_codes(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/mpccom_transaction_code_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Financial_Transactions",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_mpccom_accounting_tab"]
)
def accounting_vouchers(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/mpccom_accounting_tab/",
                                           columns_to_drop=["codeno_i", "codeno_j", 
                                                            "cost_source_id", "per_oh_adjustment_id", 
                                                            "parallel_amount", "parallel_currency_rate", 
                                                            "parallel_conversion_factor"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Financial_Transactions",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_accounting_event_tab"]
)
def accounting_event(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/accounting_event_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

NEW_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=6, minutes_to_add=15)

@asset(
    group_name="Financial_Transactions",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_inv_accounting_row_tab"],
    automation_condition=AutomationCondition.on_cron(
        NEW_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def invoicing(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/inv_accounting_row_tab/",
                                           columns_to_drop=["amount", "account_string", "code_i", 
                                                            "vat_final_posting_item_id", "sub_contract_no", 
                                                            "valuation_no", "vou_comp_amounts", 
                                                            "vou_comp_rate", "parallel_curr_rate", 
                                                            "parallel_div_factor", "deliv_type_id", 
                                                            "invoicing_advice_id"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Financial_Transactions",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_man_supp_invoice_order_tab"]
)
def supplier_invoicing(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/man_supp_invoice_order_tab/",
                                           columns_to_drop=["receipt_ref", "invoicing_advice_id"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Financial_Transactions",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_jt_task_accounting_tab"]
)
def work_task_invoicing(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/jt_task_accounting_tab/",
                                           columns_to_drop=["code_f", "code_i", "code_j", "cost_code", 
                                                            "parallel_amount", "parallel_curr_rate", 
                                                            "cost_source_id", "adjustment_id", 
                                                            "rental_transaction_id", "manually_changed", 
                                                            "changed_date", "changed_by", "user_group"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

