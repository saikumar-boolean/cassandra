from dagster import asset, AssetExecutionContext, EnvVar, AutomationCondition
from .utils import run_custom_query

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()

@asset(
    group_name="Dim_KOB",
    required_resource_keys={"snowflake_resource"},
    automation_condition=AutomationCondition.on_cron(PIPELINES_CRON, 'America/New_York')
)
def kob(context: AssetExecutionContext) -> None:
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()

    run_custom_query(context=context, query=f"""CREATE OR REPLACE TABLE {datawarehouse}.{schema}.KOB (
                                                    SK_KOB NUMBER(4,0) NOT NULL,
                                                    KOB STRING);""")
    
    kob_values = ["ADMIN", "KOB1", "KOB2", "KOB3", "STO"]
    for i in range(1, 6):
           run_custom_query(context=context, query=f"""INSERT INTO {datawarehouse}.{schema}.KOB
                              (SK_KOB, KOB) VALUES ({i}, '{kob_values[i - 1]}');""")