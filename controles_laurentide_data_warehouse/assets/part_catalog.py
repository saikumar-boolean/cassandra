import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    name="part_catalog",
    group_name="Inventory",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "ifs_part_catalog_tab",
        "ifs_part_catalog_cft"
    ]
)
def part_catalog(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    stock = generic_extraction_and_cleaning(context, "ifs/part_catalog_tab/", s3).rename(columns=str.upper)
    cft = generic_extraction_and_cleaning(context, "ifs/part_catalog_cft/", s3).rename(columns=str.upper)

    df = stock.merge(cft, on="ROWKEY", how="left")

    columns_to_drop = [
        "CUST_WARRANTY_ID",
        "SUP_WARRANTY_ID",
        "INPUT_UNIT_MEAS_GROUP_ID",
        "STORAGE_WIDTH_REQUIREMENT",
        "STORAGE_HEIGHT_REQUIREMENT",
        "STORAGE_DEPTH_REQUIREMENT",
        "STORAGE_VOLUME_REQUIREMENT",
        "UOM_FOR_LENGTH",
        "UOM_FOR_VOLUME",
        "STORAGE_WEIGHT_REQUIREMENT",
        "UOM_FOR_WEIGHT",
        "MIN_STORAGE_TEMPERATURE",
        "MAX_STORAGE_TEMPERATURE",
        "UOM_FOR_TEMPERATURE",
        "MIN_STORAGE_HUMIDITY",
        "MAX_STORAGE_HUMIDITY",
        "CAPACITY_REQ_GROUP_ID",
        "CONDITION_REQ_GROUP_ID",
        "CABABILITY_REQ_GROUP_ID",
        "TECHNICAL_DRAWING_NO",
        "CF$_LCL_APPS10_CREATED"
    ]

    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    ensure_snowflake_schema_matches_df(context, df)

    return df
