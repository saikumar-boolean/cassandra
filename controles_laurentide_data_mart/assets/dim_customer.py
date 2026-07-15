from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Customer",
    required_resource_keys={"snowflake_resource"},
    deps=["customer"]
)
def dim_customer(context: AssetExecutionContext) -> None:
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()
    datamart = EnvVar("SNOWFLAKE_DATAMART").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()


    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.DIM_CUSTOMER AS
                                                    SELECT
                                                        SK_CUSTOMER_ID AS SK_CUSTOMER_ID,
                                                        IFS_NOTES_1 AS IFS_NOTES_1,   
                                                        IFS_CUSTOMER_ID AS IFS_CUSTOMER_ID,
                                                        NAME AS NAME,
                                                        PARTY_ID AS PARTY_ID,
                                                        IFS_NOTES_2 AS IFS_NOTES_2,
                                                        LCL_SEGMENT_ID AS SEGMENT_ID,
                                                        LCL_SEGMENT AS SEGMENT,
                                                        QUEUE_ASSIGNMENT AS QUEUE_ASSIGNMENT,
                                                        Case
                                                            WHEN QUEUE_ASSIGNMENT = 'HVAC' THEN 'HAVC-OEM'
                                                            WHEN QUEUE_ASSIGNMENT = 'ENERGY' THEN 'Energy, Chemical & FnB'
                                                            WHEN QUEUE_ASSIGNMENT = 'PULP' THEN 'Pulp and Paper'
                                                            WHEN QUEUE_ASSIGNMENT = 'MINING' THEN 'Mining & Metals'
                                                            WHEN QUEUE_ASSIGNMENT = 'TIDE' THEN 'Tide'
                                                            WHEN QUEUE_ASSIGNMENT = 'PROJECT' THEN 'Emerging Market' 
                                                            ELSE NULL
                                                        END AS POD,
                                                        SALESMAN AS SALESMAN,
                                                        SALESMAN_NAME AS SALESMAN_NAME,
                                                        CUSTOMER_NO AS CUSTOMER_NO,
                                                        CUST_GRP AS CUSTOMER_GRP,
                                                        MARKET_CODE AS MARKET_CODE,
                                                        CATEGORY AS CATEGORY,
                                                        CURRENCY_CODE AS CURRENCY_CODE,
                                                        SF_CUSTOMER_ID AS SF_CUSTOMER_ID,
                                                        SF_IS_DELETED AS SF_IS_DELETED,
                                                        SF_CUSTOMER_TYPE AS SF_CUSTOME_TYPE,
                                                        SF_CUSTOMER_PARENT_ID AS SF_CUSTOMER_PARENT_ID,
                                                        SF_INDUSTRY AS SF_INDUSTRY,
                                                        SF_CREATED_DATE AS SF_CREATED_DATE,
                                                        SF_LAST_MODIFIED_DATE AS SF_LAST_MODIFIED_DATE,
                                                        SF_CATEGORY AS SF_CATEGORY,
                                                        SF_IS_SPARTAKUS AS SF_IS_SPARTAKUS,
                                                        SF_IS_PHS AS SF_IS_PHS,
                                                        SF_INDUSTRY_TYPE AS SF_INDUSTRY_TYPE,
                                                        SF_ACCOUNT_CLASSIFICATION AS SF_ACCOUNT_CLASSIFICATION,
                                                        SF_HEADQUARTERS AS SF_HEADQUARTERS,
                                                        SF_INDUSTRY_SEGMENT_ID AS SF_INDUSTRY_SEGMENT,
                                                        SF_INDUSTRY_SEGMENT_SUB_ID AS SF_INDUSTRY_SEGMENT_SUB,
                                                        SF_SALES_OFFICE_ID AS SF_SALES_OFFICE,
                                                        SF_DESCRIPTION AS SF_DESCRIPTION,
                                                        SF_LAST_ACTIVITY_DATE AS SF_LAST_ACTIVITY_DATE,
                                                        SF_ACCOUNT_RELATIONSHIP_STATUS AS SF_ACCOUNT_RELATIONSHIP_STATUS,
                                                        SF_ACCOUNT_TYPE AS SF_ACCOUNT_TYPE,
                                                        SF_NUMBER_OF_EMPLOYEES AS SF_NUMBER_OF_EMPLOYEES,
                                                        SF_CUSTOMER_ACCOUNT_NUMBER AS SF_CUSTOMER_ACCOUNT_NUMBER,
                                                        SF_PREVIOUS_OWNER AS SF_PREVIOUS_OWNER
                                                    FROM {datawarehouse}.{schema}.CUSTOMER;""")