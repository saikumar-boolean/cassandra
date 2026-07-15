from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

'''
SELECT * FROM PUR_ORD_PAY_SCHED_LINE_TAB;
empty columns: 'APPROVED_BY', 'APPROVED_DATE', 'DUE_DATE' 


SELECT * FROM PUR_ORD_PAY_SCHED_MATCH_TAB;
empty columns: 'INSTALLMENT_ID', 'MIXED_PAYMENT_ID', 'LUMP_SUM_TRANS_ID'


'''
@asset(
    name="purchase_order_pay_schedule_line",
    group_name="payables",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_pur_ord_pay_sched_line_tab"],
    metadata={
        "description": "Contains payable schedule lines by order, change order, and schedule ID.",
        "source_table": "PUR_ORD_PAY_SCHED_LINE_TAB",
        "theme": "PAYABLES",
        "domain": "Finance",
        "unique_record_type": "ORDER_NO&CHG_ORDER_NO&PAY_SCHED_ID",
        "dw_table_name": "PURCHASE_ORDER_PAY_SCHEDULE_LINE"
    }
)
def purchase_order_pay_schedule_line(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/pur_ord_pay_sched_line_tab/", s3).rename(columns=str.upper)

    columns_to_drop = ['APPROVED_BY', 'APPROVED_DATE', 'DUE_DATE' ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="purchase_order_pay_schedule",
    group_name="payables",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_pur_ord_pay_sched_match_tab"],
    metadata={
        "description": "Contains payable schedule matches by order, schedule ID, and match ID.",
        "source_table": "PUR_ORD_PAY_SCHED_MATCH_TAB",
        "theme": "PAYABLES",
        "domain": "Finance",
        "unique_record_type": "ORDER_NO&PAY_SCHED_ID&MATCH_ID",
        "dw_table_name": "PURCHASE_ORDER_PAY_SCHEDULE"
    }
)
def purchase_order_pay_schedule(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/pur_ord_pay_sched_match_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['INSTALLMENT_ID', 'MIXED_PAYMENT_ID', 'LUMP_SUM_TRANS_ID']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df
