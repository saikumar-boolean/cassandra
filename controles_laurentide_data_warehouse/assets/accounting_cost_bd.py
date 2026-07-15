from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df


'''


SELECT * FROM COST_BUCKET_TAB;
to drop: 'NOTE_TEXT', 'COST_ACTIVITY_ID', 'POSTING_GROUP_ID' 

SELECT * FROM COST_ELEMENT_TAB;
to drop: 'COST_ELEMENT_FIXED_COST', 'NOTE_TEXT', 'COST_ELEMENT_SOURCE', 'ROWKEY'

SELECT * FROM COST_ELEMENT_TO_ACCOUNT_TAB;
to drop: 'ROWKEY'

SELECT * FROM COST_SET_TAB;
to drop: 'NOTE_TEXT'

SELECT * FROM COST_TYPE_SOURCE_INDICATOR_TAB;
to drop: 'FIXED_VALUE', 'ROWKEY'

'''
@asset(
    name="cost_types_per_site",
    group_name="accounting_cost_bd",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cost_bucket_tab"]
)
def cost_bucket(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/cost_bucket_tab/", s3).rename(columns=str.upper)
    
    columns_to_drop = ['NOTE_TEXT', 'COST_ACTIVITY_ID', 'POSTING_GROUP_ID']
    #drop unnecessary columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    ensure_snowflake_schema_matches_df(context, df)

    return df

#success
@asset(
    name="cost_types",
    group_name="accounting_cost_bd",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cost_element_tab"]
    
)
def cost_element(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/cost_element_tab/", s3).rename(columns=str.upper)
    
    columns_to_drop = ['COST_ELEMENT_FIXED_COST', 'NOTE_TEXT', 'COST_ELEMENT_SOURCE', 'ROWKEY']
    #drop unnecessary columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="project_cost_elements",
    group_name="accounting_cost_bd",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cost_element_to_account_tab"]
)
def cost_element_to_account(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/cost_element_to_account_tab/", s3).rename(columns=str.upper)

    columns_to_drop = ['ROWKEY']
    #drop unnecessary columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="cost_set_per_site",
    group_name="accounting_cost_bd",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cost_set_tab"]
)
def cost_set(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/cost_set_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['NOTE_TEXT']
    #drop unnecessary columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="cost_source",
    group_name="accounting_cost_bd",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cost_type_source_indicator_tab"]
)
def cost_type_source_indicator(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/cost_type_source_indicator_tab/", s3).rename(columns=str.upper)
    
    columns_to_drop = ['FIXED_VALUE', 'ROWKEY']
    #drop unnecessary columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    ensure_snowflake_schema_matches_df(context, df)

    return df
