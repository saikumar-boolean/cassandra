import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_as_built_configuration_tab"]
)
def so_part_as_built(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/as_built_configuration_tab/",
                                           columns_to_drop=['order_ref4', 'structure_obsolete_date', 
                                                            'note_text', 'owning_vendor_no', 'comp_draw_pos_no'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_costed_routing_tab"]
)
def so_part_labor_cost(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/costed_routing_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_labor_operation_load_tab"]
)
def so_labor_load(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/labor_operation_load_tab/",
                                           columns_to_drop=['comp_profile_id', 'person_id'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_routing_alternate_tab"]
)
def routing_part_alt_config(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/routing_alternate_tab/",
                                           columns_to_drop=['rout_template_id', 'note_text'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_routing_oper_work_guide_tab"]
)
def routing_part_guidelines(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/routing_oper_work_guide_tab/",
                                           columns_to_drop=['guideline_template', 'sign_off_note', 'inspect_sign_off_note', 
                                                            'emp_qualif_prof_id', 'insp_emp_qualif_prof_id', 'reference_number', 
                                                            'reference_type'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_routing_operation_cost_tab"]
)
def routing_op_cost(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/routing_operation_cost_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_routing_operation_tab"]
)
def routing_ops(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/routing_operation_tab/",
                                           columns_to_drop=['machine_no', 'std_operation_name', 
                                                            'note_text', 'source', 'comp_profile_id', 
                                                            'setup_comp_profile_id', 'reference_number', 
                                                            'reference_type'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_routing_operation_tool_tab"]
)
def routing_operation_tools(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/routing_operation_tool_tab/",
                                           columns_to_drop=['note_text'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_shop_material_alloc_tab"]
)
def so_materials(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shop_material_alloc_tab/",
                                           columns_to_drop=['operation_no', 'draw_pos_no', 'dop_order_id', 
                                                            'generate_demand_qty', 'condition_code', 'owning_vendor_no', 
                                                            'replaced_qty', 'replaces_qpa_factor', 'replaces_line_item_no',
                                                            'position_part_no', 'cro_no', 'cro_line_no', 'service_type', 
                                                            'phantom_part_no'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_shop_oper_clocking_tab"]
)
def so_clockings(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shop_oper_clocking_tab/",
                                           columns_to_drop=['note_text', 'team_id', 'created_by_team_id', 'stopped_by_team_id', 
                                                            'modified_by_team_id', 'auto_stopped', 'interruption_cause_id'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_shop_order_cost_detail_tab"]
)
def so_mat_cost(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shop_order_cost_detail_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_shop_order_costed_op_tab"]
)
def so_op_cost(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shop_order_costed_op_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_shop_order_project_cost_tab"]
)
def so_prj_cost(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shop_order_project_cost_tab/")
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_work_center_cost_tab"]
)
def work_center_cost(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/work_center_cost_tab/",
                                           columns_to_drop=['end_date'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_work_center_tab"]
)
def work_center(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/work_center_tab/",
                                           columns_to_drop=['cum_deviation', 'input_output_code', 'input_hrs_week', 
                                                            'output_hrs_week', 'production_line', 'department_no', 
                                                            'source', 'note_text', 'code_part', 'cost_center_id', 
                                                            'capacity_calc_date', 'capacity_calc_end_date'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_shop_ord_tab",
          "ifs_shop_ord_cft"]
)
def shop_order(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_shop_ord_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/shop_ord_tab/",
        columns_to_drop=['shop_ord_rout_serial', 'complete_date', 'qty_released', 'qty_diff', 
                         'partial_direction', 'partial_operation', 'priority_no', 'reject_reason', 
                         'security_class', 'source', 'source_order_no', 'source_release_no', 
                         'source_sequence_no', 'split_reason', 'mrb_number', 'balanced_cost_diff_1300', 
                         'job_id', 'resched_code', 'process_type', 'condition_code', 'lot_batch_string', 
                         'maint_level_struct', 'maint_level_rout', 'mro_visit_id', 'mro_int_ord_header', 
                         'mro_int_order', 'dispo_order_no', 'dispo_release_no', 'dispo_sequence_no', 
                         'dispo_line_item', 'mods_defined_1300', 'repairs_defined_1300', 'case_id', 
                         'task_id', 'eso_supplier', 'eso_service_type', 'cro_no', 'cro_line', 
                         'last_avail_run_date', 'park_mrb_no', 'park_change_ord_no', 'park_org_code', 
                         'park_person_id', 'park_location_no', 'balance_id', 'balance_node_id', 
                         'closed_by_reval_event_id', 'manufactured_date', 'packing_instruction_id', 
                         'mods_reviewed', 'demand_ref1', 'production_line', 'certifying_maint_org_code'])
    
    retrieve_from_s3_shop_ord_cft = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/shop_ord_cft/",
        columns_to_drop=['cf_inv_res_location', 'cf_enter_by_date', 'cf_lcl_tracking_no'])
    
    df = pd.merge(
        retrieve_from_s3_shop_ord_tab,
        retrieve_from_s3_shop_ord_cft,
        on="rowkey", how="left",
        suffixes=("_shop_ord_tab", "_shop_ord_cft"))
    
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    group_name="Shop_Order",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_shop_order_operation_tab",
          "ifs_shop_order_operation_cft"]
)
def shop_order_ops(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_shop_order_operation_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/shop_order_operation_tab/",
        columns_to_drop=['machine_no', 'buffer_time', 'scheduled_setup_time', 'outside_op_notes', 
                         'operation_priority', 'cbs_queue_time', 'group_by_note', 'cro_no', 
                         'cro_line_no', 'service_type', 'actual_setup_time', 'comp_profile_id', 
                         'setup_comp_profile_id', 'split_from_operation_no', 'operation_block_id', 
                         'scheduling_information', 'reference_number', 'reference_type', 
                         'mso_prev_op_sched_status', 'mso_scheduling_information', 'duplicated_from'])
    
    retrieve_from_s3_shop_order_operation_cft = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/shop_order_operation_cft/")
    
    df = pd.merge(
        retrieve_from_s3_shop_order_operation_tab,
        retrieve_from_s3_shop_order_operation_cft,
        on="rowkey", how="left",
        suffixes=("_shop_order_operation_tab", "_shop_order_operation_cft"))
    
    ensure_snowflake_schema_matches_df(context, df)

    return df