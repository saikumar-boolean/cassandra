from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

'''
SELECT *
FROM GEN_LED_PROJ_VOUCHER_ROW_TAB;
empty columns: PROCESS_CODE, PERIOD_ALLOCATION, AUT_CODING_PARENT_ROW, INTERNAL_ACCOUNTING, CURR_ACCOUNTING, SUMMERIZED, OLD_ROW_NO, CODE_I, 
THIRD_CURRENCY_DEBIT_AMOUNT, THIRD_CURRENCY_CREDIT_AMOUNT, MULTI_COMPANY_ID,ACCOUNTING_YEAR_REFERENCE, VOUCHER_TYPE_REFERENCE, VOUCHER_NO_REFERENCE,
PARALLEL_CURR_RATE, PARALLEL_CURR_COV_FACT, EXCLUDE_PERIODICAL_CAP

'''

@asset(
    name="gen_led_proj_voucher",
    group_name="finance_transactions",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_gen_led_proj_voucher_row_tab"]
)
def gen_led_voucher(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/gen_led_proj_voucher_row_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
    'PROCESS_CODE','PERIOD_ALLOCATION','AUT_CODING_PARENT_ROW','INTERNAL_ACCOUNTING',
    'CURR_ACCOUNTING','SUMMERIZED', 'OLD_ROW_NO','CODE_I',
    'THIRD_CURRENCY_DEBIT_AMOUNT', 'THIRD_CURRENCY_CREDIT_AMOUNT','MULTI_COMPANY_ID','ACCOUNTING_YEAR_REFERENCE',
    'VOUCHER_TYPE_REFERENCE','VOUCHER_NO_REFERENCE','PARALLEL_CURR_RATE','PARALLEL_CURR_COV_FACT','EXCLUDE_PERIODICAL_CAP'
    ]

    #drop unnecessary columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df