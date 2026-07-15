from dagster import EnvVar
from typing import Optional
from dagster_snowflake import SnowflakeResource

snowflake_pk: Optional[str] = EnvVar("SNOWFLAKE_PK").get_value(default=None)

snowflake_resource = SnowflakeResource(
    account = EnvVar("SNOWFLAKE_ACCOUNT").get_value(),
    user = EnvVar("SNOWFLAKE_USER").get_value(),
    private_key = snowflake_pk,
    database = EnvVar("SNOWFLAKE_DATAMART").get_value(),
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value(default="PUBLIC"),
    role = EnvVar("SNOWFLAKE_ROLE").get_value(),
    warehouse = EnvVar("SNOWFLAKE_WAREHOUSE").get_value(),
)