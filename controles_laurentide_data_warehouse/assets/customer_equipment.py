import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Customer_Equipment",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_equipment_obj_group_tab"]
)
def customer_equipment_group(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/equipment_obj_group_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Customer_Equipment",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_equipment_object_tab",
          "ifs_equipment_object_journal_tab",
          "ifs_equipment_object_party_tab"]
)
def customer_equipment(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_equipment_object_party_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client, 
        key="ifs/equipment_object_party_tab/")
    
    retrieve_from_s3_equipment_object_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client, 
        key="ifs/equipment_object_tab/",
        columns_to_drop=[
        "rowstate", "mch_loc", "mch_pos", "mch_doc", "part_rev", "type", "warr_exp", "mch_type",
        "cost_center", "object_no", "category_id", "main_pos", "production_date", "info", "data",
        "criticality", "plant_design_id", "plant_design_projphase", "plant_design_cotproj_projid",
        "ownership", "owner", "item_class_id", "applied_pm_program_id", "applied_pm_program_rev",
        "applied_date", "not_applicable_reason", "not_applicable_set_user", "not_applicable_set_date",
        "process_class_id", "cluster_id", "model_id", "technical_lifetime", "area_id", "deck_id",
        "maintenance_strategy_id"])
    
    retrieve_from_s3_equipment_object_journal_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client, 
        key="ifs/equipment_object_journal_tab/",
        columns_to_drop=["note", "journal_text"])
    
    merged_df = pd.merge(
        retrieve_from_s3_equipment_object_party_tab, 
        retrieve_from_s3_equipment_object_tab,
        on="equipment_object_seq", how="left", 
        suffixes=("_equipment_object_party_tab", "_equipment_object_tab"))

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_equipment_object_journal_tab,
        on="equipment_object_seq", how="left", 
        suffixes=("", "_equipment_object_journal_tab"))
    
    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df