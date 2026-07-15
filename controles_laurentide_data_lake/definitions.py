from dagster import Definitions, multiprocess_executor
import dagster as dg

from .assets import dlt_resource, generate_ifs_asset, generate_salesforce_asset, generate_ifs_asset_with_offset
from .tables import IFS_TABLES_WITHOUT_ROWVERSION, IFS_TABLES_WITH_ROWVERSION,SALESFORCE_TABLES, INCREMENTAL_IFS_TABLES

automation_sensor = dg.AutomationConditionSensorDefinition(
    name="automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = Definitions(
    assets=[
        *[generate_ifs_asset(ifs_table) for ifs_table in IFS_TABLES_WITHOUT_ROWVERSION],
        *[generate_ifs_asset(ifs_table) for ifs_table in IFS_TABLES_WITH_ROWVERSION],
        *[generate_salesforce_asset(salesforce_table) for salesforce_table in SALESFORCE_TABLES],
        *[generate_ifs_asset_with_offset(table) for table in INCREMENTAL_IFS_TABLES]
    ],
    resources={
        "dlt": dlt_resource
    },
    sensors=[automation_sensor],
    executor=multiprocess_executor
)
