from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df


@asset(
    name="bopp_quote_history",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_opportun_history_tab"]
)
def bopp_quote_history(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/business_opportun_history_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="bopp_quote_group",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_rep_group_tab"]
)
def bopp_quote_group(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/business_rep_group_tab/",
        s3
    ).rename(columns=str.upper)

    columns_to_drop = [
        "BUSINESS_ACT_PREFIX",
        "MARKET_CAMP_PREFIX",
        "BUSINESS_LEAD_PREFIX",
        "SALES_LEAD_PREFIX"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="bopp_quote_revision",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_opportun_revision_tab"]
)
def bopp_quote_revision(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/business_opportun_revision_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="bopp_quote_stages",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_rm_proc_connected_stage_tab"]
)
def bopp_quote_stages(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/rm_proc_connected_stage_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="service_quote_resources",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_jt_task_srv_quo_resource_tab"]
)
def service_quote_resources(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/jt_task_srv_quo_resource_tab/",
        s3
    ).rename(columns=str.upper)

    columns_to_drop = [
        "REMARK"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df



@asset(
    name="service_quote_tasks",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_jt_task_srv_quo_tab"]
)
def service_quote_tasks(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/jt_task_srv_quo_tab/",
        s3
    ).rename(columns=str.upper)

    columns_to_drop = [
        "WORK_STAGE_ID"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="bopp_quote_line_history",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_opp_line_history_tab"]
)
def bopp_quote_line_history(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/business_opp_line_history_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df



@asset(
    name="bopp_quote_representative",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_representative_tab"]
)
def bopp_quote_representative(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/business_representative_tab/",
        s3
    ).rename(columns=str.upper)


    columns_to_drop = [
        "BUSINESS_ACT_PREFIX",
        "MARKET_CAMP_PREFIX",
        "BUSINESS_LEAD_PREFIX",
        "OUTLOOK_SYNC_ACTIVE"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df



@asset(
    name="bopp_quote_representative_group",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_rep_group_member_tab"]
)
def bopp_quote_representative_group(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    df = generic_extraction_and_cleaning(
        context,
        "ifs/business_rep_group_member_tab/",
        s3
    ).rename(columns=str.upper)

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="bopp_quote_line",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=[
        "ifs_business_opportunity_line_tab",
        "ifs_business_opportunity_line_cft"
    ]
)
def bopp_quote_line(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    line_tab = generic_extraction_and_cleaning(context, "ifs/business_opportunity_line_tab/", s3).rename(columns=str.upper)
    line_cft = generic_extraction_and_cleaning(context, "ifs/business_opportunity_line_cft/", s3).rename(columns=str.upper)

    df = line_tab.merge(line_cft, on="ROWKEY", how="left", suffixes=("", "_cft"))

    columns_to_drop = [
        "CONFIGURED_LINE_PRICE_ID", "CALC_CHAR_PRICE", "CHAR_PRICE"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="bopp_quote",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_opportunity_tab", "business_opportunity_cft"]
)
def bopp_quote(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    opp_tab = generic_extraction_and_cleaning(context, "ifs/business_opportunity_tab/", s3).rename(columns=str.upper)
    opp_cft = generic_extraction_and_cleaning(context, "ifs/business_opportunity_cft/", s3).rename(columns=str.upper)

    df = opp_tab.merge(opp_cft, on="ROWKEY", how="left", suffixes=("", "_cft"))

    columns_to_drop = [
        "OPPORTUNITY_TYPE", "MAIN_OPPORTUN_REFERENCE", "SALES_LEAD_ID",
        "LEAD_ID", "PARENT_BUSINESS_OBJECT_ID"
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="quote_contact",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_business_object_contact_tab", "business_object_contact_cft"]
)
def quote_contact(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    contact_tab = generic_extraction_and_cleaning(context, "ifs/business_object_contact_tab/", s3).rename(columns=str.upper)
    contact_cft = generic_extraction_and_cleaning(context, "ifs/business_object_contact_cft/", s3).rename(columns=str.upper)

    df = contact_tab.merge(contact_cft, on="ROWKEY", how="left", suffixes=("", "_cft"))

    columns_to_drop = [
        "NOTE_TEXT", "OBJECT_ROLE", "OBJECT_DECISION_POWER_TYPE"
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="bopp_quote_actions",
    group_name="Quoting",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_rm_proc_connected_action_tab", "rm_proc_action_tab"]
)
def bopp_quote_actions(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    connected = generic_extraction_and_cleaning(context, "ifs/rm_proc_connected_action_tab/", s3).rename(columns=str.upper)
    action = generic_extraction_and_cleaning(context, "ifs/rm_proc_action_tab/", s3).rename(columns=str.upper)

    df = connected.merge(
        action,
        on=["PROCESS_ID", "STAGE_ID", "ACTION_ID"],
        how="left",
        suffixes=("", "_1")
    )

    columns_to_drop = [
        "DATA_ENTRY_LU_LINK", "APPROVED_BY", "APPROVED_DATE", "APPROVAL_GROUP_ID",
        "DATA_ENTRY_LU_LINK_1", "APPROVAL_GROUP_ID_1", "VALIDATION_TYPE_1", "ROWVERSION_1", "DESCRIPTION_1", "DATA_ENTRY_LU_NAME_1",
        "DATA_ENTRY_LU_COLUMNS_1", "ACTIVITY_DEMAND_1", "ROWKEY_1"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df