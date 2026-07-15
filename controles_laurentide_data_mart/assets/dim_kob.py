from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Dim_KOB",
    required_resource_keys={"snowflake_resource"},
    deps=["kob"]
)
def dim_kob(context: AssetExecutionContext) -> None:
    datamart = EnvVar("SNOWFLAKE_DATAMART").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()

    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.DIM_KOB AS
                                                SELECT
                                                    SK_KOB AS SK_KOB, 
                                                    KOB AS KOB
                                                FROM {datawarehouse}.{schema}.KOB;""")