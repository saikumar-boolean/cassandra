from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

'''
SELECT * FROM OPERATION_HISTORY_TAB;
empty cols: 'MODIFY_DATE_APPLIED_DATE', 'MODIFY_DATE_APPLIED_USER', 'TEAM_ID',
'CREATED_BY_TEAM_ID', 'REPORT_POINT_ID', 'PURCH_SEQUENCE_NO', 'COMP_PROFILE_ID', 'SETUP_COMP_PROFILE_ID', 'TRANSPORT_DEL_NOTE_NO'
'''

@asset(
    name="shop_order_ops_hist",
    group_name="shop_orders",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_operation_history_tab"],
    metadata={
        "description": "Operational history of shop orders with transactional details.",
        "source_table": "OPERATION_HISTORY_TAB",
        "theme": "SHOP ORDERS",
        "domain": "Assembly / Operations",
        "unique_record_type": "TRANSACTION_ID",
        "dw_table_name": "SHOP_ORDER_OPS_HIST",
        "key": "TRANSACTION_ID"
    }
)
def shop_order_ops_hist(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/operation_history_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['MODIFY_DATE_APPLIED_DATE', 'MODIFY_DATE_APPLIED_USER', 'TEAM_ID', 'CREATED_BY_TEAM_ID', 'REPORT_POINT_ID', 'PURCH_SEQUENCE_NO', 'COMP_PROFILE_ID', 'SETUP_COMP_PROFILE_ID', 'TRANSPORT_DEL_NOTE_NO']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df
