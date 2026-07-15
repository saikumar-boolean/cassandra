from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Part",
    required_resource_keys={"snowflake_resource"},
    deps=["part"]
)
def dim_part(context: AssetExecutionContext) -> None:
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()
    datamart = EnvVar("SNOWFLAKE_DATAMART").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()

    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.DIM_PART AS
                                                    SELECT
                                                        SK_PART_ID AS SK_PART_ID,
                                                        PART_NO AS PART_NO,
                                                        DESCRIPTION AS DESCRIPTION,
                                                        PART_MAIN_GROUP AS PART_MAIN_GROUP,
                                                        INFO_TEXT AS INFO_TEXT,
                                                        UNIT_CODE AS UNIT_CODE,
                                                        C_MANUFACTURER AS MANUFACTURER,
                                                        C_MANUF_PART_NO AS MANUFACTURER_PART_NO,
                                                        C_LIST_PRICE AS LIST_PRICE,
                                                        C_DEAL_NO AS DEAL_NO,
                                                        C_APPLICATION_CODE AS APPLICATION_CODE,
                                                        C_EFFECTIVE_PRICE_DATE AS EFECTIVE_PRICE_DATE,
                                                        C_DIVISION_ID AS DIVISION_ID,
                                                        C_LINE_TYPE AS LINE_TYPE,
                                                        C_CREATE_USER AS CREATE_USER,
                                                        C_CREATE_DATE AS CREATE_DATE,
                                                        C_MODIFY_USER AS MODIFY_USER,
                                                        C_MODIFY_DATE AS MODIFY_DATE,
                                                        C_PART_PRODUCT_CODE AS PRODUCT_CODE,
                                                        C_PRODUCT_GROUP AS PRODUCT_GROUP,
                                                        C_PRODUCT_LINE AS PRODUCT_LINE,
                                                        SALES_GROUP AS SALES_GROUP,
                                                        SALES_PRICE_GROUP AS SALES_PRICE_GROUP,
                                                        BUSINESS_UNIT AS BUSINESS_UNIT,
                                                        CATEGORY_1 AS CATEGORY_1,
                                                        CATEGORY_2 AS CATEGORY_2
                                                    FROM {datawarehouse}.{schema}.PART;""")