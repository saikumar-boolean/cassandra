from dagster import EnvVar
from dagster_snowflake_pandas import SnowflakePandasIOManager
from typing import Optional
from pyarrow import fs
from dagster_snowflake import SnowflakeResource

snowflake_pk: Optional[str] = EnvVar("SNOWFLAKE_PK").get_value(default=None)

snowflake_io_manager = SnowflakePandasIOManager(
    account = EnvVar("SNOWFLAKE_ACCOUNT").get_value(),
    user = EnvVar("SNOWFLAKE_USER").get_value(),
    private_key = snowflake_pk,
    database = EnvVar("SNOWFLAKE_DATABASE").get_value(),
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value(default="PUBLIC"),
    role = EnvVar("SNOWFLAKE_ROLE").get_value(),
    warehouse = EnvVar("SNOWFLAKE_WAREHOUSE").get_value(),
)


s3_client = fs.S3FileSystem(
        access_key = EnvVar("WAREHOUSE_AWS_ACCESS_KEY_ID").get_value(),
        secret_key = EnvVar("WAREHOUSE_AWS_SECRET_ACCESS_KEY").get_value(),
        region = EnvVar("WAREHOUSE_AWS_REGION").get_value(),
    )

snowflake_resource = SnowflakeResource(
    account = EnvVar("SNOWFLAKE_ACCOUNT").get_value(),
    user = EnvVar("SNOWFLAKE_USER").get_value(),
    private_key = snowflake_pk,
    database = EnvVar("SNOWFLAKE_DATABASE").get_value(),
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value(default="PUBLIC"),
    role = EnvVar("SNOWFLAKE_ROLE").get_value(),
    warehouse = EnvVar("SNOWFLAKE_WAREHOUSE").get_value(),
)

snowflake_resource_test = SnowflakeResource(
    account = EnvVar("SNOWFLAKE_ACCOUNT").get_value(),
    user = EnvVar("SNOWFLAKE_USER").get_value(),
    private_key = snowflake_pk,
    database = EnvVar("SNOWFLAKE_DATABASE_TEST").get_value(),
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value(default="PUBLIC"),
    role = EnvVar("SNOWFLAKE_ROLE").get_value(),
    warehouse = EnvVar("SNOWFLAKE_WAREHOUSE").get_value(),
)

snowflake_io_manager_test = SnowflakePandasIOManager(
    account = EnvVar("SNOWFLAKE_ACCOUNT").get_value(),
    user = EnvVar("SNOWFLAKE_USER").get_value(),
    private_key = snowflake_pk,
    database = EnvVar("SNOWFLAKE_DATABASE_TEST").get_value(),
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value(default="PUBLIC"),
    role = EnvVar("SNOWFLAKE_ROLE").get_value(),
    warehouse = EnvVar("SNOWFLAKE_WAREHOUSE").get_value(),
)