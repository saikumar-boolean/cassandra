import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Contacts",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_crm_person_info_history_tab"]
)
def contact_history(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/crm_person_info_history_tab/",
                                           columns_to_drop=['interaction_description'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Contacts",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_crm_cust_info_tab"]
)
def contacts(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/crm_cust_info_tab/",
                                           columns_to_drop=['turnover', 'year_end_period', 'source_id', 'potential_id', 'loyalty_id', 'employee_count_id', 
                                                            'competitor_id', 'note', 'parent_company', 'parent_category', 'account_type'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df