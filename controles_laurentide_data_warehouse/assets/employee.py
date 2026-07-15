import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Employee",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_work_location_category_tab"]
)
def work_location_category(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
            s3=context.resources.s3_client, 
            key="ifs/work_location_category_tab/",
            columns_to_drop=['rowkey'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df
    
@asset(
    group_name="Employee",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_current_sched_temp_tab"]
)
def current_sched_temp(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
            s3=context.resources.s3_client, 
            key="ifs/current_sched_temp_tab/",
            columns_to_drop=['absence_fday', 'absence_from_time', 'absence_tday', 'absence_to_time', 'absence_hours', 'absence_wage_code', 'shift'])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Employee",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_day_sched_sub_desc_tab",
          "ifs_day_sched_sub_tab"]
)
def employee_schedule(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_day_sched_sub_desc_tab = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/day_sched_sub_desc_tab/",
                                           columns_to_drop=['work_hours_sum'])
    
    retrieve_from_s3_day_sched_sub_tab = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/day_sched_sub_tab/",
                                           columns_to_drop=['rowkey'])
    
    df = pd.merge(
        retrieve_from_s3_day_sched_sub_desc_tab, 
        retrieve_from_s3_day_sched_sub_tab,
        on="day_sched_sub_code", how="left", 
        suffixes=("_day_sched_sub_desc_tab", "_day_sched_sub_tab"))

    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    group_name="Employee",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=["ifs_bu_access_temp_tab",
        "ifs_bu_access_valid_tab",
        "ifs_business_unit_relation_tab",
        "ifs_business_unit_tab",
        "ifs_company_emp_tab",
        "ifs_company_pers_assign_tab",
        "ifs_company_person_cft",
        "ifs_company_person_tab",
        "ifs_company_position_structure_610",
        "ifs_company_position_tab",
        "ifs_emp_employed_time_tab",
        "ifs_employee_group_assignments_tab",
        "ifs_employee_work_location_tab",
        "ifs_work_sched_assign_tab"]
)
def employee(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_bu_access_temp_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/bu_access_temp_tab/",
        columns_to_drop=['access_role_id', 'substitute_seq_id']
    )

    retrieve_from_s3_bu_access_valid_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/bu_access_valid_tab/",
        columns_to_drop=['access_role_id', 'substitute_seq_id']
    )

    retrieve_from_s3_business_unit_relation_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/business_unit_relation_tab/",
        columns_to_drop=['struct_bu_id', 'trans_from', 'substitute_entry', 'substitute_seq_id']
    )

    retrieve_from_s3_business_unit_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/business_unit_tab/",
        columns_to_drop=['business_unit_code', 'name', 'org_term_id', 'org_terminal_code', 'is_root', 'person_group_id']
    )

    retrieve_from_s3_company_emp_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/company_emp_tab/",
    )

    retrieve_from_s3_company_pers_assign_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/company_pers_assign_tab/",
        columns_to_drop=['vacancy_request_id', 'applicable_to_cost_center', 'distribution_percent']
    )

    retrieve_from_s3_company_person_cft = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/company_person_cft/",
        columns_to_drop=['cf_lcl_apd_no']
    )

    retrieve_from_s3_company_person_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/company_person_tab/",
        columns_to_drop=['emp_remark', 'emp_public_remark', 'emp_card', 'company_office', 'area_code', 'created_by_mcpr']
    )

    retrieve_from_s3_company_position_structure_610 = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/company_position_structure_610/",
        columns_to_drop=['rowkey']
    )

    retrieve_from_s3_company_position_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/company_position_tab/",
        columns_to_drop=['pos_cat', 'job_id_147629']
    )

    retrieve_from_s3_emp_employed_time_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/emp_employed_time_tab/",
        columns_to_drop=['previous_employment_info', 'agreement_id']
    )

    retrieve_from_s3_employee_group_assignments_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/employee_group_assignments_tab/",
    )

    retrieve_from_s3_employee_work_location_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/employee_work_location_tab/",
        columns_to_drop=['notes']
    )

    retrieve_from_s3_work_sched_assign_tab = generic_extraction_and_cleaning(
        context=context,
        s3=context.resources.s3_client,
        key="ifs/work_sched_assign_tab/",
        columns_to_drop=['increment_sched_code', 'emergency_sched_code', 'normal_hours_generation', 'inc_start_seq_no']
    )

    merged_df = pd.merge(
        retrieve_from_s3_business_unit_tab, 
        retrieve_from_s3_business_unit_relation_tab,
        on=["rowkey"], how="left", 
        suffixes=("_business_unit_tab", "_business_unit_relation_tab"))
    
    retrieve_from_s3_bu_access_temp_tab = retrieve_from_s3_bu_access_temp_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_bu_access_temp_tab,
        on=["emp_no", "company_id"], how="left", 
        suffixes=("", "_bu_access_temp_tab"))

    retrieve_from_s3_bu_access_valid_tab = retrieve_from_s3_bu_access_valid_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_bu_access_valid_tab,
        on=["emp_no", "company_id"], how="left",
        suffixes=("", "_bu_access_valid_tab"))
    
    retrieve_from_s3_company_pers_assign_tab = retrieve_from_s3_company_pers_assign_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_company_pers_assign_tab,
        on=["emp_no", "company_id"], how="left",
        suffixes=("", "_company_pers_assign_tab"))

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_company_emp_tab,
        left_on=["emp_no", "company_id"], right_on=["employee_id", "company"], how="left",
        suffixes=("", "_company_emp_tab"))
    
    retrieve_from_s3_company_person_tab = retrieve_from_s3_company_person_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_company_person_tab,
        on=["emp_no", "company_id"], how="left",
        suffixes=("", "_company_person_tab"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_company_person_cft,
        left_on=["rowkey_company_person_tab"], right_on=["rowkey"], how="left",
        suffixes=("", "_company_person_cft"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_company_position_structure_610,
        on=["pos_code"], how="left",
        suffixes=("", "_company_position_structure_610"))
    
    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_company_position_tab,
        on=["pos_code"], how="left",
        suffixes=("", "_company_position_tab"))
    
    retrieve_from_s3_emp_employed_time_tab = retrieve_from_s3_emp_employed_time_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_emp_employed_time_tab,
        on=["emp_no", "company_id"], how="left",
        suffixes=("", "_emp_employed_time_tab"))
    
    retrieve_from_s3_employee_group_assignments_tab = retrieve_from_s3_employee_group_assignments_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_employee_group_assignments_tab,
        on=["emp_no", "company_id"], how="left",
        suffixes=("", "_employee_group_assignments_tab"))

    retrieve_from_s3_employee_work_location_tab = retrieve_from_s3_employee_work_location_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_employee_work_location_tab,
        on=["emp_no", "company_id"], how="left",
        suffixes=("", "_employee_work_location_tab"))    
    
    retrieve_from_s3_work_sched_assign_tab = retrieve_from_s3_work_sched_assign_tab.dropna(subset=["emp_no", "company_id"])

    merged_df = pd.merge(
        merged_df, 
        retrieve_from_s3_work_sched_assign_tab,
        on=["emp_no", "company_id"], how="left",
        suffixes=("", "_work_sched_assign_tab"))     
    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df