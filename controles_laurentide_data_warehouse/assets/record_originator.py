import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Record_Originator",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_bus_obj_representative_tab"]
)
def bus_obj_representative(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/bus_obj_representative_tab/",
                                           columns_to_drop=[])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Record_Originator",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_rm_acc_priv_user_tab"]
)
def rm_acc_priv_user(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/rm_acc_priv_user_tab/",
                                           columns_to_drop=[])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df
