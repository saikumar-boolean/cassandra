from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df
import pandas as pd

# @asset(
#     name="pick_lists",
#     group_name="picking",
#     io_manager_key="snowflake_io_manager",
#     required_resource_keys={"s3_client", "snowflake_resource"},
#     deps=["pick_list_line_tab", "pick_list_tab"]
# )
# def pick_lists(context: AssetExecutionContext) -> pd.DataFrame:
#     s3 = context.resources.s3_client
#
#     line = generic_extraction_and_cleaning(context, "ifs/pick_list_line_tab/", s3)
#     header = generic_extraction_and_cleaning(context, "ifs/pick_list_tab/", s3)
#
#
#     line.columns = [col.upper().strip() for col in line.columns]
#     header.columns = [col.upper().strip() for col in header.columns]
#
#
#     to_drop = [col for col in header.columns if col in line.columns and col != "PICK_LIST_NO"]
#     header = header.drop(columns=to_drop)
#
#     df = line.merge(header, on="PICK_LIST_NO", how="left")
#
#
#     df.columns = [col.upper().strip().replace(" ", "_").replace("-", "_") for col in df.columns]
#
#     ensure_snowflake_schema_matches_df(context, df)
#     return df