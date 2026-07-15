from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, rename_columns, map_column_values, add_surrogate_key, ensure_snowflake_schema_matches_df
@asset(
    group_name="Customer",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_cust_info_p000007944",
          "ifs_customer_info_cft",
          "ifs_customer_info_tab",
          "ifs_cust_ord_customer_tab",
          "salesforce_account"]
)
def customer(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_cust_info_p000007944 = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/cust_info_p000007944/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
            "corporate_form", "identifier_reference", "picture_id", "customer_tax_usage_type",
            "business_classification", "date_of_registration", "cf_cc_surcharge", "cf_custom_instruction",
            "cf_lcl_integration_date", "cf_lcl_integration_msg", "cf_lcl_integration_remark",
            "cf_lcl_integration_status", "cf_lcl_integration_user", "cf_lcl_salesforce_id",
            "cf_nan_estore_site_def", "cf_nan_estore_site_def_db", "cf_nan_rep_id_db",
            "cf_nan_rep_id", "cf_paretositeid", "cf_xt_surcharge"]
    )
    keep_cols_cust_info_p000007944 = ['customer_id', 'cf_customer_type', 'cf_lcl_notes', 'cf_lcl_segment_db', 'cf_lcl_segment', 
                 'cf_queue_assignment', 'cf_salesman', 'cf_salesman_name']
    
    retrieve_from_s3_cust_info_p000007944 = retrieve_from_s3_cust_info_p000007944[keep_cols_cust_info_p000007944]
    retrieve_from_s3_cust_info_p000007944 = rename_columns(df=retrieve_from_s3_cust_info_p000007944, old_cols=keep_cols_cust_info_p000007944,
                                                           new_cols=['IFS_CUSTOMER_ID', 'IFS_CUSTOMER_TYPE', 'IFS_NOTES_2', 'LCL_SEGMENT_ID', 'LCL_SEGMENT', 
                                                                     'QUEUE_ASSIGNMENT', 'SALESMAN', 'SALESMAN_NAME'])
    mapping_QUEUE_ASSIGNMENT = {
        'HAVC': 'HAVC-OEM',
        'OEM': 'HAVC-OEM',
        'Energy_Chemical_DnB': 'Energy',
        'Pulp_and_Paper': 'Pulp and Paper',
        'Mining_Metals': 'Mining & Metals',
        'Tide': 'Tide',
        'Projects': 'Emerging Market'
    }
    
    retrieve_from_s3_cust_info_p000007944 = map_column_values(df=retrieve_from_s3_cust_info_p000007944, column='QUEUE_ASSIGNMENT', mapping=mapping_QUEUE_ASSIGNMENT)
    
    retrieve_from_s3_customer_info_cft = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/customer_info_cft/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
            "cf_cc_surcharge", "cf_xt_surcharge", "cf_paretositeid", "cf_custom_instruction",
            "cf_nan_estore_site_def", "cf_lcl_integration_user"]
    )

    keep_cols_customer_info_cft = ['cf_lcl_notes', 'rowkey', 'cf_lcl_salesforce_id']
    

    retrieve_from_s3_customer_info_cft = retrieve_from_s3_customer_info_cft[keep_cols_customer_info_cft]


    retrieve_from_s3_customer_info_cft = rename_columns(df=retrieve_from_s3_customer_info_cft, old_cols=keep_cols_customer_info_cft, 
                   new_cols=["IFS_NOTES_1", "ROWKEY", "CF_LCL_SALESFORCE_ID"])
    
    retrieve_from_s3_customer_info_tab = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/customer_info_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
            "corporate_form", "identifier_reference", "picture_id", "customer_tax_usage_type",
            "business_classification", "date_of_registration"]
    )

    keep_cols_customer_info_tab = ['customer_category', 'one_time', 'customer_id', 'name',
                                   'creation_date', 'party', 'default_domain', 'default_language', 'country', 'rowkey' ]
    
    retrieve_from_s3_customer_info_tab = retrieve_from_s3_customer_info_tab[keep_cols_customer_info_tab]
    retrieve_from_s3_customer_info_tab = rename_columns(df=retrieve_from_s3_customer_info_tab, old_cols=keep_cols_customer_info_tab, 
                   new_cols=['IFS_CUSTOMER_CATEGORY', 'IFS_ONE_TIME', 'IFS_CUSTOMER_ID', 'NAME',
                             'IFS_CREATION_DATE', 'PARTY_ID', 'DEFAULT_DOMAIN', 'DEFAULT_LANGUAGE', 'COUNTRY', 'ROWKEY' ])
    
    retrieve_from_s3_customer_info_tab['IFS_CREATION_DATE'] = pd.to_datetime(retrieve_from_s3_customer_info_tab['IFS_CREATION_DATE']).dt.date
    retrieve_from_s3_customer_info_tab['IFS_CREATION_DATE'] = pd.to_datetime(retrieve_from_s3_customer_info_tab['IFS_CREATION_DATE']).dt.strftime('%Y-%m-%d')

    retrieve_from_s3_cust_ord_customer_tab = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/cust_ord_customer_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
            "cust_price_group_id", "customer_no_pay", "print_control_code", "last_ivc_date",
            "vat", "template_id", "discount_type", "discount", "no_delnote_copies",
            "forward_agent_id", "rec_adv_auto_approval_user", "sbi_auto_approval_user", "priority"]
    )

    keep_cols_cust_ord_customer_tab = ['edi_auto_change_approval', 'release_internal_order', 'backorder_option', 'customer_no', 'cust_grp', 
                         'market_code', 'cust_ref', 'category', 'currency_code', 'edi_auto_order_approval', 'commission_receiver']

    retrieve_from_s3_cust_ord_customer_tab = retrieve_from_s3_cust_ord_customer_tab[keep_cols_cust_ord_customer_tab]
    retrieve_from_s3_cust_ord_customer_tab = rename_columns(df=retrieve_from_s3_cust_ord_customer_tab, old_cols=keep_cols_cust_ord_customer_tab,
                   new_cols=['EDI_AUTO_CHANGE_APPROVAL', 'RELEASE_INTERNAL_ORDER', 'BACKORDER_OPTION', 'CUSTOMER_NO', 'CUST_GRP', 'MARKET_CODE', 
                             'CUST_REF', 'CATEGORY', 'CURRENCY_CODE', 'EDI_AUTO_ORDER_APPROVAL', 'COMMISSION_RECEIVER'])

    retrieve_from_s3_account = generic_extraction_and_cleaning(
        context = context,
        key = "salesforce/account/",
        s3 = context.resources.s3_client
    )

    keep_cols_account = ['id', 'is_deleted', 'type', 'parent_id', 'phone', 'website', 'industry', 'currency_iso_code', 'owner_id', 
                         'created_date', 'created_by_id', 'last_modified_date', 'last_modified_by_id', 'queue_assignment__c', 'category__c', 
                         'spartakus__c', 'phs_account__c', 'industry_type__c', 'account_classification__c', 'headquarters__c', 
                         'industry_segment__c', 'industry_sub_segment__c', 'sales_office__c', 'description', 'last_activity_date', 
                         'account_relationship_status__c', 'account_type__c', 'number_of_employees', 'account_number', 'previous_owner__c', 
                         'market_segment__c', 'business_drivers__c', 'business_description__c', 'status__c', 'fiscal_year_start_date__c', 'fax']

    retrieve_from_s3_account = retrieve_from_s3_account[keep_cols_account]
    retrieve_from_s3_account = rename_columns(df=retrieve_from_s3_account, old_cols=keep_cols_account,
                   new_cols=['SF_CUSTOMER_ID', 'SF_IS_DELETED', 'SF_CUSTOMER_TYPE', 'SF_CUSTOMER_PARENT_ID', 'PHONE', 
                             'WEBSITE', 'SF_INDUSTRY', 'SF_CURRENCY_ISO_CODE', 'SF_OWNER_ID', 'SF_CREATED_DATE', 
                             'SF_CREATED_BY_ID', 'SF_LAST_MODIFIED_DATE', 'SF_LAST_MODIFIED_BY_ID', 'SF_QUEUE_ASSIGNMENT', 
                             'SF_CATEGORY', 'SF_IS_SPARTAKUS', 'SF_IS_PHS', 'SF_INDUSTRY_TYPE', 'SF_ACCOUNT_CLASSIFICATION', 
                             'SF_HEADQUARTERS', 'SF_INDUSTRY_SEGMENT_ID', 'SF_INDUSTRY_SEGMENT_SUB_ID', 'SF_SALES_OFFICE_ID', 
                             'SF_DESCRIPTION', 'SF_LAST_ACTIVITY_DATE', 'SF_ACCOUNT_RELATIONSHIP_STATUS', 'SF_ACCOUNT_TYPE', 
                             'SF_NUMBER_OF_EMPLOYEES', 'SF_CUSTOMER_ACCOUNT_NUMBER', 'SF_PREVIOUS_OWNER', 'SF_MARKET_SEGMENT', 
                             'SF_BUSINESS_DRIVER', 'SF_BUSINESS_DESCRIPTION', 'SF_STATUS', 'SF_FISCAL_YEAR_START_DATE', 'FAX'])
    
    mapping_Queue_Assignment__c = {
        'HAVC': 'HAVC-OEM',
        'OEM': 'HAVC-OEM',
        'Energy, Chemical & FnB': 'Energy',
        'Pulp and Paper': 'Pulp and Paper',
        'Mining & Metal': 'Mining & Metals',
        'ACL Order Desk': 'Tide',
        'Emerging Market': 'Emerging Market'
    }
    retrieve_from_s3_account = map_column_values(df=retrieve_from_s3_account, column='SF_QUEUE_ASSIGNMENT', mapping=mapping_Queue_Assignment__c)

    merged_df = pd.merge(
        retrieve_from_s3_customer_info_tab,
        retrieve_from_s3_customer_info_cft,
        on="ROWKEY", how="left",
        suffixes=("_customer_info_tab", "_customer_info_cft"))

    merged_df = pd.merge(
        merged_df,
        retrieve_from_s3_account,
        left_on="CF_LCL_SALESFORCE_ID", right_on="SF_CUSTOMER_ID", how="left",
        suffixes=("", "_account"))

    merged_df = pd.merge(
        merged_df,
        retrieve_from_s3_cust_ord_customer_tab,
        left_on="IFS_CUSTOMER_ID", right_on="CUSTOMER_NO", how="left",
        suffixes=("", "_cust_ord_customer_tab"))

    merged_df = pd.merge(
        merged_df,
        retrieve_from_s3_cust_info_p000007944,
        on="IFS_CUSTOMER_ID", how="left",
        suffixes=("", "_cust_info_p000007944"))

    merged_df = add_surrogate_key(merged_df, key_name="SK_CUSTOMER_ID", by=["IFS_CUSTOMER_ID"])

    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df

@asset(
    group_name="Customer",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_customer_info_address_tab", "ifs_cust_ord_customer_address_tab"]
)
def customer_address(context: AssetExecutionContext) -> pd.DataFrame:
    retrieve_from_s3_customer_info_address_tab = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/customer_info_address_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=[
            "secondary_contact", "jurisdiction_code", "end_customer_id", "end_cust_addr_id",
            "comm_id", "address3", "address4", "address5", "address6", "customer_branch"]
    )

    retrieve_from_s3_cust_ord_customer_address_tab = generic_extraction_and_cleaning(
        context = context,
        key = "ifs/cust_ord_customer_address_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=["load_sequence_no", "route_id", "delivery_time"]
    )

    merged_df = pd.merge(
        retrieve_from_s3_customer_info_address_tab,
        retrieve_from_s3_cust_ord_customer_address_tab,
        left_on=["customer_id", "address_id"], right_on=["customer_no", "addr_no"],
        how='inner', suffixes=("_customer_info_address_tab", "_cust_ord_customer_tab"))

    
    ensure_snowflake_schema_matches_df(context, merged_df)

    return merged_df

@asset(
    group_name="Customer",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_customer_info_address_type_tab"]
)
def customer_address_type(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context = context,
        key = "ifs/customer_info_address_type_tab/",
        s3 = context.resources.s3_client)
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Customer",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_sales_region_tab"]
)
def customer_geo_region(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context = context,
        key = "ifs/sales_region_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=["rowkey"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Customer",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_sales_district_tab"]
)
def customer_industry_type(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context = context,
        key = "ifs/sales_district_tab/",
        s3 = context.resources.s3_client,
        columns_to_drop=["rowkey"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df
