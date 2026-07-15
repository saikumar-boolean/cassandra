from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Dim_Date",
    required_resource_keys={"snowflake_resource"},
    deps=["date"]
)
def dim_date(context: AssetExecutionContext) -> None:
    datamart = EnvVar("SNOWFLAKE_DATAMART").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()

    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.DIM_DATE AS
                                                    SELECT
                                                        DATE_KEY                AS DATE_KEY,
                                                        DATE                    AS DATE,
                                                        DAY                     AS DAY,
                                                        DAY_OF_WEEK             AS DAY_OF_WEEK,
                                                        WEEK_OF_YEAR            AS WEEK_OF_YEAR,
                                                        MONTH_NUMBER            AS MONTH_NUMBER,
                                                        MONTH_NAME              AS MONTH_NAME,
                                                        QUARTER                 AS QUARTER,
                                                        YEAR                    AS YEAR,
                                                        FISCAL_MONTH_NUMBER     AS FISCAL_MONTH_NUMBER,
                                                        FISCAL_QUARTER          AS FISCAL_QUARTER,
                                                        FISCAL_YEAR             AS FISCAL_YEAR
                                                    FROM {datawarehouse}.{schema}.DATE;""")