from dagster import asset, AssetExecutionContext
import pandas as pd
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df


'''
SELECT * FROM AUD_AUDIT_AUTHORITY_TAB;
to drop: 'NOTES', 

SELECT * FROM AUD_AUDIT_AUTHORITY_TYPE_TAB;
to drop: 'NOTES'

SELECT *
FROM AUD_AUDIT_AUTHORITY_TAB auth
LEFT JOIN AUD_AUDIT_AUTHORITY_TYPE_TAB type
  ON auth.AUTHORITY_TYPE_ID = type.AUTHORITY_TYPE_ID;


  
SELECT * FROM AUD_AUDIT_GROUP_TAB;
to drop: 'NOTES'

SELECT * FROM AUD_AUDIT_TAB;
to drop: only one entry, not sure what to drop

SELECT *
FROM AUD_AUDIT_TAB audit
LEFT JOIN AUD_AUDIT_GROUP_TAB grp
  ON audit.AUDIT_GROUP_ID = grp.AUDIT_GROUP_ID;




SELECT * FROM AUD_AUDIT_TYPE_TAB;
to drop: 'NOTES'



SELECT * FROM AUD_BUSINESS_AREA_TAB
to drop: 'NOTES'



SELECT * FROM AUD_INTERNAL_AUDITOR_TAB
to drop: 'NOTES'

'''


@asset(
    name="audit_auth_type",
    group_name="quality_audit",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_aud_audit_authority_tab", "ifs_aud_audit_authority_type_tab"],
    metadata={
        "description": "Audit authorities with their corresponding authority types.",
        "source_table": "AUD_AUDIT_AUTHORITY_TAB + AUD_AUDIT_AUTHORITY_TYPE_TAB",
        "theme": "QUALITY AUDIT",
        "domain": "Quality",
        "unique_record_type": "AUTHORITY_TYPE_ID",
        "dw_table_name": "AUDIT_AUTH_TYPE"
    }
)
def audit_auth_type(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    auth = generic_extraction_and_cleaning(context, "ifs/aud_audit_authority_tab/", s3).rename(columns=str.upper)
    type_ = generic_extraction_and_cleaning(context, "ifs/aud_audit_authority_type_tab/", s3).rename(columns=str.upper)

    type_ = type_.drop(columns=[col for col in ["NOTES", 'DESCRIPTION', 'ROWKEY', 'ROWSTATE', 'ROWVERSION' , '_DLT_ID', '_DLT_LOAD_ID'] if col in type_.columns])
    df = auth.merge(type_, on="AUTHORITY_TYPE_ID", how="left", suffixes=("", "_type"))

    columns_to_drop = ["NOTES"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="audit_by_group",
    group_name="quality_audit",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_aud_audit_tab", "ifs_aud_audit_group_tab"],
    metadata={
        "description": "Audits joined to their associated audit group metadata.",
        "source_table": "AUD_AUDIT_TAB + AUD_AUDIT_GROUP_TAB",
        "theme": "QUALITY AUDIT",
        "domain": "Quality",
        "unique_record_type": "AUDIT_GROUP_ID",
        "dw_table_name": "AUDIT_BY_GROUP"
    }
)
def audit_by_group(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    audit = generic_extraction_and_cleaning(context, "ifs/aud_audit_tab/", s3).rename(columns=str.upper)
    group = generic_extraction_and_cleaning(context, "ifs/aud_audit_group_tab/", s3).rename(columns=str.upper)

    group = group.drop(columns=[col for col in ["NOTES", 'ROWKEY', 'ROWSTATE', 'ROWVERSION','_DLT_ID' ,'_DLT_LOAD_ID'] if col in group.columns])
    df = audit.merge(group, on="AUDIT_GROUP_ID", how="left", suffixes=("", "_group"))

    columns_to_drop = [] 
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="audit_types",
    group_name="quality_audit",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_aud_audit_type_tab"],
    metadata={
        "description": "Master list of audit types (e.g., compliance, quality).",
        "source_table": "AUD_AUDIT_TYPE_TAB",
        "theme": "QUALITY AUDIT",
        "domain": "Quality",
        "unique_record_type": "AUDIT_TYPE_ID",
        "dw_table_name": "AUDIT_TYPES"
    }
)
def audit_types(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/aud_audit_type_tab/", s3).rename(columns=str.upper)

    columns_to_drop = ['NOTES']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="audit_business_area",
    group_name="quality_audit",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_aud_business_area_tab"],
    metadata={
        "description": "Audit business area configuration.",
        "source_table": "AUD_BUSINESS_AREA_TAB",
        "theme": "QUALITY AUDIT",
        "domain": "Quality",
        "unique_record_type": "BUSINESS_AREA_ID",
        "dw_table_name": "AUDIT_BUSINESS_AREA"
    }
)
def audit_business_area(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/aud_business_area_tab/", s3).rename(columns=str.upper)

    columns_to_drop = ['NOTES']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df

@asset(
    name="auditors",
    group_name="quality_audit",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_aud_internal_auditor_tab"],
    metadata={
        "description": "Internal auditors set up for quality audit assignments.",
        "source_table": "AUD_INTERNAL_AUDITOR_TAB",
        "theme": "QUALITY AUDIT",
        "domain": "Quality",
        "unique_record_type": "AUDITOR_ID",
        "dw_table_name": "AUDITORS"
    }
)
def auditors(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client
    df = generic_extraction_and_cleaning(context, "ifs/aud_internal_auditor_tab/", s3).rename(columns=str.upper)

    columns_to_drop = ['NOTES']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    ensure_snowflake_schema_matches_df(context, df)

    return df
