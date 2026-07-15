from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Dim_Salesman",
    required_resource_keys={"snowflake_resource"},
    deps=["salesman"]
)
def dim_salesman(context: AssetExecutionContext) -> None:
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()
    datamart = EnvVar("SNOWFLAKE_DATAMART").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()

    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.DIM_SALESMAN AS
                                                    SELECT 
                                                        SK_SALESMAN_ID                                              AS SALESMAN_ID,
                                                        SALESMAN_CODE                                               AS SALESMAN_CODE,
                                                        BLOCKED_FOR_USE                                             AS BLOCKED_FOR_USE,
                                                        ROWVERSION                                                  AS ROWVERSION,
                                                        ROWSTATE                                                    AS ROWSTATE,
                                                        OFFICE                                                      AS OFFICE,
                                                        DOS                                                         AS DOS,
                                                        UPPER(LEFT(SPLIT_PART(SALESMAN_CODE, '.', 1), 1)) 
                                                            || LOWER(SUBSTR(SPLIT_PART(SALESMAN_CODE, '.', 1), 2))  AS FIRST_NAME,
                                                        UPPER(LEFT(SPLIT_PART(SALESMAN_CODE, '.', 2), 1)) 
                                                            || LOWER(SUBSTR(SPLIT_PART(SALESMAN_CODE, '.', 2), 2))  AS LAST_NAME
                                                    FROM {datawarehouse}.{schema}.SALESMAN;""")