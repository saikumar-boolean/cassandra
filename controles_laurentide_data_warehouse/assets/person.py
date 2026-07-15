import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Person",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_pers_tab",
          "ifs_person_info_tab"]
)
def persons(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_pers_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/pers_tab/",
        columns_to_drop=['name3', 'name5', 'name6', 'insurance_id', 'blood_type'])
    
    retrieve_from_s3_person_info_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/person_info_tab/",
        columns_to_drop=['prefix', 'birth_name', 'alias', 'external_display_name',
                          'internal_display_name', 'picture_thumbnail_id', 'alternative_name'])
    
    df = pd.merge(
        retrieve_from_s3_person_info_tab, 
        retrieve_from_s3_pers_tab,
        on="person_id", how="left", 
        suffixes=("_person_info_tab", "_pers_tab"))
    
    ensure_snowflake_schema_matches_df(context, df)

    return df