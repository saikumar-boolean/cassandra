from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df


'''
SELECT * FROM INVENTORY_PICK_LIST_TAB;
empty columns: none


SELECT * FROM warehouse_tab;
empty columns: 'BIN_HEIGHT_CAPACITY', 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY', 'BIN_VOLUME_CAPACITY', 'BIN_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE', 
'BIN_MAX_TEMPERATURE', 'BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY', 'BAY_CARRYING_CAPACITY', 'ROW_CARRYING_CAPACITY', 'TIER_CARRYING_CAPACITY', 'AVAILABILITY_CONTROL_ID'
'PALLET_DROP_OFF_LOCATION_NO', 'PUTAWAY_ZONE_RANKING', 'PUTAWAY_MAX_BINS_PER_PART', 'TRANSPORT_FROM_WHSE_LEVEL', 'TRANSPORT_TO_WHSE_LEVEL', 'WAREHOUSE_TYPE_ID'

SELECT * FROM WAREHOUSE_BAY_TAB;
empty columns: 'BIN_HEIGHT_CAPACITY' , 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY', 'BIN_VOLUME_CAPACITY', 'BIN_CARRYING_CAPACITY', 'BAY_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE'
'BIN_MAX_TEMPERATURE', 'BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY', 'ROW_CARRYING_CAPACITY', 'TIER_CARRYING_CAPACITY'
'AVAILABILITY_CONTROL_ID', 'DROP_OFF_LOCATION_NO', 'PALLET_DROP_OFF_LOCATION_NO'

SELECT * FROM WAREHOUSE_BAY_BIN_TAB;
empty columns: 'HEIGHT_CAPACITY', 'WIDTH_CAPACITY', 'DEPT_CAPACITY', 'VOLUME_CAPACITY', 'CARRYING_CAPACITY',
 'MIN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_HUMIDITY', 'MAX_HUMIDITY', 'AVAILABILITY_CONTROL_ID'

SELECT * FROM WAREHOUSE_BAY_ROW_TAB;
empty columns: 'BIN_HEIGHT_CAPACITY', 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY',
'BIN_VOLUME_CAPACITY', 'BIN_CARRYING_CAPACITY', 'ROW_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE',
 'BIN_MAX_TEMPERATURE','BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY' ,'AVAILABILITY_CONTROL_ID'

SELECT * FROM WAREHOUSE_BAY_TIER_TAB;
empty columns: 'DESCRIPTION', 'BIN_HEIGHT_CAPACITY', 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY', 'BIN_VOLUME_CAPACITY', 'BIN_CARRYING_CAPACITY', 'TIER_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE', 'BIN_MAX_TEMPERATURE', 'BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY', 'AVAILABILITY_CONTROL_ID'
'''
@asset(
    name="inventory_picklist",
    group_name="warehouse",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_inventory_pick_list_tab"]
)
def inventory_picklist(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/inventory_pick_list_tab/", s3).rename(columns=str.upper)
    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="warehouse",
    group_name="warehouse",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_warehouse_tab"]
)
def warehouse(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/warehouse_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['BIN_HEIGHT_CAPACITY', 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY', 'BIN_VOLUME_CAPACITY',
                       'BIN_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE', 'BIN_MAX_TEMPERATURE', 'BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY', 'BAY_CARRYING_CAPACITY',
                       'ROW_CARRYING_CAPACITY', 'TIER_CARRYING_CAPACITY', 'AVAILABILITY_CONTROL_ID''PALLET_DROP_OFF_LOCATION_NO', 'PUTAWAY_ZONE_RANKING',
                       'PUTAWAY_MAX_BINS_PER_PART', 'TRANSPORT_FROM_WHSE_LEVEL', 'TRANSPORT_TO_WHSE_LEVEL', 'WAREHOUSE_TYPE_ID']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="warehouse_bay",
    group_name="warehouse",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_warehouse_bay_tab"]
)
def warehouse_bay(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/warehouse_bay_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['BIN_HEIGHT_CAPACITY' , 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY', 'BIN_VOLUME_CAPACITY', 'BIN_CARRYING_CAPACITY', 'BAY_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE'
        'BIN_MAX_TEMPERATURE', 'BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY', 'ROW_CARRYING_CAPACITY', 'TIER_CARRYING_CAPACITY'
        'AVAILABILITY_CONTROL_ID', 'DROP_OFF_LOCATION_NO', 'PALLET_DROP_OFF_LOCATION_NO']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="warehouse_bay_bin",
    group_name="warehouse",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_warehouse_bay_bin_tab"]
)
def warehouse_bay_bin(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/warehouse_bay_bin_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['HEIGHT_CAPACITY', 'WIDTH_CAPACITY', 'DEPT_CAPACITY', 'VOLUME_CAPACITY', 'CARRYING_CAPACITY','MIN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_HUMIDITY', 'MAX_HUMIDITY', 'AVAILABILITY_CONTROL_ID']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="warehouse_bay_row",
    group_name="warehouse",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_warehouse_bay_row_tab"]
)
def warehouse_bay_row(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/warehouse_bay_row_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['BIN_HEIGHT_CAPACITY', 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY','BIN_VOLUME_CAPACITY', 'BIN_CARRYING_CAPACITY', 'ROW_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE','BIN_MAX_TEMPERATURE','BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY' ,'AVAILABILITY_CONTROL_ID']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df


@asset(
    name="warehouse_bay_tier",
    group_name="warehouse",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_warehouse_bay_tier_tab"]
)
def warehouse_bay_tier(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/warehouse_bay_tier_tab/", s3).rename(columns=str.upper)
    columns_to_drop = ['DESCRIPTION', 'BIN_HEIGHT_CAPACITY', 'BIN_WIDTH_CAPACITY', 'BIN_DEPT_CAPACITY', 'BIN_VOLUME_CAPACITY', 'BIN_CARRYING_CAPACITY', 'TIER_CARRYING_CAPACITY', 'BIN_MIN_TEMPERATURE', 'BIN_MAX_TEMPERATURE', 'BIN_MIN_HUMIDITY', 'BIN_MAX_HUMIDITY', 'AVAILABILITY_CONTROL_ID']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df


