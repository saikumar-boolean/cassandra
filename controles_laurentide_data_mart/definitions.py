import os
from dotenv import load_dotenv
import importlib
import pkgutil
import dagster as dg
from dagster import Definitions, multiprocess_executor

from controles_laurentide_data_mart import resources, assets


load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

asset_modules = []
package_path = assets.__path__ 

for _, module_name, _ in pkgutil.iter_modules(package_path):
    full_module_name = f"{assets.__name__}.{module_name}"
    module = importlib.import_module(full_module_name)
    asset_modules.append(module)

all_asset_groups = [
    dg.load_assets_from_modules(
        [mod],
        automation_condition=dg.AutomationCondition.eager()
    ) 
    for mod in asset_modules
]
flattened_assets = [asset for group in all_asset_groups for asset in group]

automation_sensor = dg.AutomationConditionSensorDefinition(
    name="automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = Definitions(
    assets=[*flattened_assets],
    resources={
        "snowflake_resource": resources.snowflake_resource
    },
    sensors=[automation_sensor],
    executor=multiprocess_executor
)
