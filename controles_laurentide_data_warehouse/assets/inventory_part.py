import pandas as pd
from dagster import asset, EnvVar, AssetExecutionContext, AutomationCondition, AssetSelection
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df, add_shift_cron_time

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()
NEW_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=6, minutes_to_add=30)

@asset(
    name="inventory_part",
    group_name="Inventory",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "ifs_inventory_part_tab",
        "ifs_inv_part_emission_head_tab",
        "ifs_inventory_part_cft",
        "ifs_inventory_part_config_tab",
        "ifs_inventory_part_planner_tab",
        "ifs_inventory_part_planning_tab",
        "ifs_inventory_part_unit_cost_tab"
    ],
    automation_condition=AutomationCondition.on_cron(
        NEW_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def inventory_part(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    ipt = generic_extraction_and_cleaning(context, "ifs/inventory_part_tab/", s3, 
                                          columns_to_drop=['accounting_group', 'hazard_code', 'dim_quality', 
                                                           'customs_stat_no', 'eng_attribute', 'intrastat_conv_factor',
                                                            'part_cost_group_id', 'std_name_id', 'technical_coordinator_id', 
                                                            'actual_cost_activated', 'max_actual_cost_update', 'cust_warranty_id', 
                                                            'sup_warranty_id', 'region_of_origin', 'supply_chain_part_group', 
                                                            'input_unit_meas_group_id', 'storage_width_requirement', 
                                                            'storage_height_requirement', 'storage_depth_requirement', 
                                                            'storage_volume_requirement', 'storage_weight_requirement', 
                                                            'min_storage_temperature', 'max_storage_temperature', 
                                                            'min_storage_humidity', 'max_storage_humidity', 'standard_putaway_qty', 
                                                            'abc_class_locked_until', 'life_stage_locked_until', 
                                                            'freq_class_locked_until', 'acquisition_reason_id', 
                                                            'acquisition_origin', 'statistical_code', 'hsn_sac_code', 
                                                            'product_category_id'])
    
    ipem = generic_extraction_and_cleaning(context, "ifs/inv_part_emission_head_tab/", s3, 
                                           columns_to_drop=['effective_date', 'effective_serial_no', 'calculation_guid', 
                                                           'calculation_date', 'note_text', 'declared_mass_percent', 
                                                           'approved_date', 'rohs_first_supply_date', 'rohs_exemption', 
                                                           'rohs_directive', 'no_of_compl_comp', 'no_of_non_compl_comp', 
                                                           'no_of_exempt_comp', 'no_of_unknown_comp', 'no_of_na_comp', 
                                                           'no_of_declared_comp', 'no_of_compl_comp_ml', 'no_of_non_compl_comp_ml', 
                                                           'no_of_exempt_comp_ml', 'no_of_unknown_comp_ml', 'no_of_na_comp_ml', 
                                                           'no_of_declared_comp_ml', 'scip_no', 'submission_no', 'emission_template_id',
                                                           "rowkey", "rowversion", "last_activity_date"])
    
    conf = generic_extraction_and_cleaning(context, "ifs/inventory_part_config_tab/", s3, 
                                           columns_to_drop=['inventory_value', 'accumulated_purchase_diff', "rowversion",
                                                            'accumulated_manuf_diff', 'last_actual_cost_calc', 'last_manuf_cost_calc'])
    
    cft = generic_extraction_and_cleaning(context, "ifs/inventory_part_cft/", s3, columns_to_drop=['cf_nan_call_to_buy'])
    
    planner = generic_extraction_and_cleaning(context, "ifs/inventory_part_planner_tab/", s3, columns_to_drop=['buyer_title', "rowkey", "rowversion", "rowstate"])
    
    plan = generic_extraction_and_cleaning(context, "ifs/inventory_part_planning_tab/", s3, columns_to_drop=['order_trip_date', 'qty_predicted_consumption',
                                                                                                             "rowversion", "last_activity_date"])
    
    uc = generic_extraction_and_cleaning(context, "ifs/inventory_part_unit_cost_tab/", s3, columns_to_drop=["rowkey", "rowversion", "configuration_id"])

    df = ipt \
        .merge(cft, on="rowkey", how="left") \
        .merge(ipem, on=["part_no", "contract"], how="left") \
        .merge(conf, on=["part_no", "contract"], how="left", suffixes=("", "_config")) \
        .merge(planner, left_on="planner_buyer", right_on="buyer_code", how="left", suffixes=("", "_planner")) \
        .merge(plan, on=["part_no", "contract"], how="left", suffixes=("", "_plan")) \
        .merge(uc, on=["part_no", "contract"], how="left", suffixes=("", "_unitcost"))

    ensure_snowflake_schema_matches_df(context, df)

    return df
