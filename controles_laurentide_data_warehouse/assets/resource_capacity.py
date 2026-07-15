from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

'''
SQL
SELECT * FROM
RESOURCE_CAPACITY_TAB;

to drop: EXCEPTION_TYPE_2120
'''

@asset(
    name="resource_capacity",
    group_name="planning",  # based on PLANNING domain
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_resource_capacity_tab"]
)
def resource_capacity(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/resource_capacity_tab/", s3).rename(columns=str.upper)
    
    columns_to_drop = ['EXCEPTION_TYPE_2120']
    #drop unnecessary columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
