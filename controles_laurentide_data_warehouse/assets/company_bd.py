import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Company_BD",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_company_tab"]
)
def company(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/company_tab/",
                                           columns_to_drop=['association_no', 'corporate_form', 
                                                            'authorization_id', 'auth_id_expire_date', 
                                                            'activity_start_date', 'identifier_reference', 
                                                            'business_classification'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Company_BD",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_site_tab"]
)
def site(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/site_tab/",
                                           columns_to_drop=['data_capture_menu_id', 'time_zone_code'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df