import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    name="currency_history",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_currency_rate_tab"]
)
def currency_history(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/currency_rate_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="account_group",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_account_group_tab"]
)
def account_group(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/account_group_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
        "DEFAULT_CONS_ACCNT_2110",
        "DEF_MASTER_COMPANY_ACCNT"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="accounting_balance",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_accounting_balance_tab"]
)
def accounting_balance(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/accounting_balance_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [

    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="accounting_period",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_accounting_period_tab"]
)
def accounting_period(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/accounting_period_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
                "REPORT_FROM_DATE",
                "REPORT_UNTIL_DATE",
                "ATTRIBUTE_ONE",
                "ATTRIBUTE_TWO",
                "ATTRIBUTE_THREE",
                "ATTRIBUTE_FOUR",
                "ATTRIBUTE_FIVE"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="accounting_process_code",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_accounting_process_code_tab"]
)
def accounting_process_code(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/accounting_process_code_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [

    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df



@asset(
    name="pre_accounting",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_pre_accounting_tab"]
)
def pre_accounting(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/pre_accounting_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
            "PARENT_PRE_ACCOUNTING_ID",
            "AMOUNT_DISTRIBUTION",
            "CODENO_I",
            "CODENO_J",
            "CODENO_H",
            "CODENO_B",
            "ACTIVITY_SEQ"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="transaction_account",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_c_inventory_transactions_tab"]
)
def transaction_account(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(context, "ifs/c_inventory_transactions_tab/", s3).rename(columns=str.upper)

    columns_to_drop = [
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df



@asset(
    name="currency_revaluation",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "ifs_currency_reval_detail_tab",
        "ifs_currency_reval_trans_tab",
        "ifs_currency_revaluation_tab"
    ]
)
def currency_revaluation(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

 
    detail = generic_extraction_and_cleaning(context, "ifs/currency_reval_detail_tab/", s3).rename(columns=str.upper)
    trans = generic_extraction_and_cleaning(context, "ifs/currency_reval_trans_tab/", s3).rename(columns=str.upper)
    reval = generic_extraction_and_cleaning(context, "ifs/currency_revaluation_tab/", s3).rename(columns=str.upper)


    df = detail.merge(
        trans,
        on=["COMPANY", "REVALUATION_ID", "ACCOUNTING_YEAR", "ACCOUNTING_PERIOD", "ROW_NO"],
        how="left",
        suffixes=("", "_1")
    )


    df = df.merge(
        reval,
        on=["COMPANY", "REVALUATION_ID", "ACCOUNTING_YEAR", "ACCOUNTING_PERIOD"],
        how="left",
        suffixes=("", "_2")
    )


    columns_to_drop = [
        "CODE_C", "CODE_H", "CODE_I", "REVAL_TEXT", "PARALLEL_CURRENCY_RATE",
        "REVALUED_AMOUNT_PAR", "AMOUNT_BEF_REVAL_PAR", "REVAL_GAIN_PAR", "REVAL_LOSS_PAR",
        "CODE_C_1", "CODE_F_1", "CODE_I_1", "CODE_J_1", "REVAL_TEXT_1",
        "PARALLEL_CURRENCY_RATE_1", "REVALUED_AMOUNT_PAR_1", "AMOUNT_BEF_REVAL_PAR_1",
        "REVAL_GAIN_PAR_1", "REVAL_LOSS_PAR_1", "POSTED_PARALLEL_RATE", "REVAL_TEXT_2",
        "PARALLED_CURRENCY_TYPE", "UNREALIZED_GAIN_IN_PAR", "UNREALIZED_LOSS_IN_PAR"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="part_values",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "ifs_accounting_code_part_value_tab",
        "ifs_accounting_codestr_compl_tab",
        "ifs_product_code_x_ref_clt"
    ]
)
def part_values(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    acpv = generic_extraction_and_cleaning(context, "ifs/accounting_code_part_value_tab/", s3).rename(columns=str.upper)
    accodestr = generic_extraction_and_cleaning(context, "ifs/accounting_codestr_compl_tab/", s3).rename(columns=str.upper)
    prodxref = generic_extraction_and_cleaning(context, "ifs/product_code_x_ref_clt/", s3).rename(columns=str.upper)


    accodestr = accodestr.drop(columns=[
    col for col in ["ROWVERSION"] if col in accodestr.columns]) 

    prodxref = prodxref.drop(columns=[
    col for col in ["ROWKEY", "ROWVERSION"] if col in prodxref.columns])

    df = acpv \
        .merge(accodestr, on=["CODE_PART_VALUE", "CODE_PART", "COMPANY"], how="left", suffixes=("", "_compl")) \
        .merge(prodxref, left_on="ROWKEY", right_on="CF_LCL_BU", how="left", suffixes=("", "_xref"))

    columns_to_drop = [
        "ACCOUNTING_TEXT_ID", "PROCESS_CODE", "CURR_RATE", "REVENUE_RECOG_METHOD",
        "REVISION_ACCOUNTING", "POC_METHOD", "PROJECT_PROGESS_METHOD", "CONTRIB_MARGIN",
        "PROJECT_LEADER", "PARTY", "GRANT_ALLOWANCE", "UNIT_PRICE", "ORDER_NUMBER",
        "POSTING_COMBINATION_ID", "CONS_CODE_PART_VALUE_2110", "VOUCHER_DATE",
        "NCF_OVERRIDE_FEE", "NCF_INV_STAT_FEE", "FINALLY_INVOICED", "FINALLY_INVO_ACC_PERIOD",
        "CLOSED_ACC_PERIOD", "VOUCHER_TYPE", "USER_GROUP", "CODE_B", "CODE_C", "CODE_D", "CODE_E",
        "CODE_F", "CODE_G", "CODE_H", "CODE_I", "CODE_J", "REPORTING_ENTITY", "MASTER_COM_CODE_PART_VALUE",
        "SAT_ACCOUNT_TYPE", "SAT_ACCOUNT_GROUP", "SAT_LEVEL", "SAT_PARENT_ACCOUNT", "ROWKEY_2",
        "CUSTOMER_NO", "CONTRACT", "ORDER_NO"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="posting_controls",
    group_name="Finance_data",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "posting_ctrl_comb_detail_tab",
        "posting_ctrl_detail_tab",
        "posting_ctrl_tab"
    ]
)
def posting_controls(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client


    comb = generic_extraction_and_cleaning(context, "ifs/posting_ctrl_comb_detail_tab/", s3).rename(columns=str.upper)
    detail = generic_extraction_and_cleaning(context, "ifs/posting_ctrl_detail_tab/", s3).rename(columns=str.upper)
    ctrl = generic_extraction_and_cleaning(context, "ifs/posting_ctrl_tab/", s3).rename(columns=str.upper)

 
    valid_control_types = set(detail["CONTROL_TYPE"].dropna().unique()).union(
        set(ctrl["CONTROL_TYPE"].dropna().unique())
    )


    comb = comb[
        comb["CONTROL_TYPE1"].isin(valid_control_types) |
        comb["CONTROL_TYPE2"].isin(valid_control_types)
    ]

  
    detail1 = detail.rename(columns=lambda x: f"{x}_DETAIL1")
    df = comb.merge(
        detail1,
        left_on=["COMPANY", "POSTING_TYPE", "CODE_PART", "PC_VALID_FROM", "CONTROL_TYPE1"],
        right_on=["COMPANY_DETAIL1", "POSTING_TYPE_DETAIL1", "CODE_PART_DETAIL1", "PC_VALID_FROM_DETAIL1", "CONTROL_TYPE_DETAIL1"],
        how="left"
    )


    detail2 = detail.rename(columns=lambda x: f"{x}_DETAIL2")
    df = df.merge(
        detail2,
        left_on=["COMPANY", "POSTING_TYPE", "CODE_PART", "PC_VALID_FROM", "CONTROL_TYPE2"],
        right_on=["COMPANY_DETAIL2", "POSTING_TYPE_DETAIL2", "CODE_PART_DETAIL2", "PC_VALID_FROM_DETAIL2", "CONTROL_TYPE_DETAIL2"],
        how="left"
    )


    df = df.merge(
        ctrl.rename(columns=lambda x: f"{x}_CTRL"),
        left_on=["COMPANY", "POSTING_TYPE", "CODE_PART", "PC_VALID_FROM"],
        right_on=["COMPANY_CTRL", "POSTING_TYPE_CTRL", "CODE_PART_CTRL", "PC_VALID_FROM_CTRL"],
        how="left"
    )


    columns_to_drop = [  
    "CONTROL_TYPE_DETAIL1", "CONTROL_TYPE_DETAIL2", "CONTROL_TYPE_CTRL",
    
    "MODULE_DETAIL1", "MODULE_DETAIL2", "MODULE_CTRL",
    "NO_CODE_PART_VALUE_DETAIL1", "NO_CODE_PART_VALUE_DETAIL2",
    "OVERRIDE_CTRL",
    
    "PC_VALID_FROM_DETAIL1", "PC_VALID_FROM_DETAIL2", "PC_VALID_FROM_CTRL",
    "POSTING_TYPE_DETAIL1", "POSTING_TYPE_DETAIL2", "POSTING_TYPE_CTRL",
    "COMPANY_DETAIL1", "COMPANY_DETAIL2", "COMPANY_CTRL",
    "CODE_PART_DETAIL1", "CODE_PART_DETAIL2", "CODE_PART_CTRL",
    
    "ROWKEY_DETAIL1", "ROWKEY_DETAIL2", "ROWKEY_CTRL",
    "ROWVERSION_DETAIL1", "ROWVERSION_DETAIL2", "ROWVERSION_CTRL",
    "VALID_FROM_DETAIL1", "VALID_FROM_DETAIL2",
    
    "_DLT_ID_DETAIL1", "_DLT_ID_DETAIL2", "_DLT_ID_CTRL",
    "_DLT_LOAD_ID_DETAIL1", "_DLT_LOAD_ID_DETAIL2", "_DLT_LOAD_ID_CTRL"
]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df







