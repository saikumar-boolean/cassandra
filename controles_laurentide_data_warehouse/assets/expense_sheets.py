from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

'''
Expense Sheet Header SQL:
SELECT * 
FROM EXPENSE_HEADER_TAB
LEFT JOIN TRIP_HEADER_TAB
ON TRIP_HEADER_TAB.ROWKEY = EXPENSE_HEADER_TAB.ROWKEY;

Expense Sheet Detail SQL:
SELECT * 
FROM EXPENSE_DETAIL_TAB
LEFT JOIN TRIP_DETAIL_TAB
ON TRIP_DETAIL_TAB.ROWKEY = EXPENSE_DETAIL_TAB.ROWKEY;

'''

@asset(
    name="expense_sheet_details",
    group_name="expense_sheets", #adjusted based o
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    #list of tables that are dependant HAS TOP BE MINISCULE
    tags={"tblinvoiced": ""},
    deps=["ifs_trip_detail_tab",
          "ifs_expense_detail_tab"]
)

#define table to be pushed on dagster/snowflake
def expense_sheet_details(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    tdt = generic_extraction_and_cleaning(context, "ifs/trip_detail_tab/", s3).rename(columns=str.upper)
    edt = generic_extraction_and_cleaning(context, "ifs/expense_detail_tab/", s3).rename(columns=str.upper)

    tdt = tdt.drop(columns=[col for col in ["PRICE", "VAT_AMOUNT","TRIP_DETAIL_ID","COMPANY_ID", "QUANTITY","EXPENSE_OBJECT_ID","CURRENCY_CODE", "CURR_RATE", "EXPENSE_CODE", "EXPENSE_RULE", "EXPENSE_TYPE", "GROSS_AMOUNT", "REFERENCE", "REIMBURSABLE","ROWVERSION", "SEQ_NO", "_DLT_ID" ,"_DLT_LOAD_ID" ] if col in tdt.columns])

    df = edt.merge(tdt, on="ROWKEY", how="left", suffixes=("", "_trip"))

    columns_to_drop = [
        'CALCULATED', 'CODE_F', 'CODE_H', 'CODE_I', 'AREA', 'USER_EXPENSE_CODE', 'CREDIT_CARD_TRANS_ID',
        'USER_SEQ_NO', 'CALC_CURR_AMOUNT', 'CALC_PAY_AMOUNT', 'EXPENSE_CUST_OBJECT_ID', 'SUPPLIER_ID', 'TAX_ID_NUMBER', 'SAT_TYPE', 'COMPANY_ID_1', 'EMP_NO', 'TRIP_ID',
        'TRIP_DETAIL_ID_1', 'EXPENSE_DATE', 'EXPENSE_TYPE_1', 'EXPENSE_RULE_1', 'SEQ_NO_1', 'EXPENSE_CODE_1', 'REIMBURSABLE_1', 'CURRENCY_CODE_1', 'CURR_RATE_1', 'GROSS_AMOUNT_1', 'VAT_AMOUNT_1', 'QUANTITY_1', 'PRICE_1', 'REFERENCE_1',
        'EXPENSE_OBJECT_ID_1', 'TRIP_STATUS', 'ROWVERSION_1', 'ROWKEY_1'
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="expense_sheet_header",
    group_name="expense_sheets", #adjusted based o
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    #list of tables that are dependant HAS TOP BE MINISCULE
    tags={"tblinvoiced": ""},
    deps=["ifs_expense_header_tab",
          "trip_header_tab"]
)

#define table to be pushed on dagster/snowflake
def expense_sheet_header(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    tht = generic_extraction_and_cleaning(context, "ifs/trip_header_tab/", s3).rename(columns=str.upper)
    eht = generic_extraction_and_cleaning(context, "ifs/expense_header_tab/", s3).rename(columns=str.upper)

    tht = tht.drop(columns=[col for col in ["COMPANY_ID", "EMP_NO", "EXPENSE_RULE", "EXPENSE_OBJECT_ID", "EXPENSE_STATUS", "PURPOSE", "ROWVERSION", "_DLT_ID", "_DLT_LOAD_ID"] if col in tht.columns])

    df = eht.merge(tht, on="ROWKEY", how="left", suffixes=("", "_trip"))

    columns_to_drop = [
        'CURRENCY_RATE', 'TRANS_ID', 'ADVANCE_AMOUNT', 'REQUEST_ID',
        'PAY_PAR_CURR_RATE', 'PAY_PAR_CONV_FACTOR',
        'EXPENSE_CUST_OBJECT_ID', 'COMPANY_ID_1', 'EMP_NO_1', 'TRIP_ID_1', 'EXPENSE_RULE_1', 'PURPOSE_1',
        'INFORMATION', 'TRIP_TYPE', 'TRIP_STATUS', 'EXPENSE_STATUS_1', 'START_DATE', 'END_DATE', 'LEFT_HOME_DATE', 'BACK_HOME_DATE', 'EXPENSE_OBJECT_ID_1', 'AMOUNT',
        'REIMBURSE_AMOUNT', 'ROWVERSION_1', 'ROWKEY_1'
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
