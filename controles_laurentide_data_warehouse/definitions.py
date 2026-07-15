import os
from dotenv import load_dotenv
import importlib
import pkgutil
import dagster as dg
from dagster import Definitions, multiprocess_executor

from controles_laurentide_data_warehouse import resources, assets
from controles_laurentide_data_warehouse.assets import date as date_module, kob as kob_module

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

date_assets = dg.load_assets_from_modules([date_module])
kob_assets = dg.load_assets_from_modules([kob_module])

excluded_from_eager = ["sales_invoice_line", "invoicing", "invoice_lines", "inventory_value", "inventory_part", "sales_part", "inventory_transaction", "purchase_transaction", "payable_transaction"]

asset_modules = []
package_path = assets.__path__ 

for _, module_name, _ in pkgutil.iter_modules(package_path):
    full_module_name = f"{assets.__name__}.{module_name}"
    module = importlib.import_module(full_module_name)
    asset_modules.append(module)

other_asset_modules = [mod for mod in asset_modules if mod != date_module and mod != kob_module]

flattened_assets = []
for mod in other_asset_modules:
    module_assets = []
    for name in dir(mod):
        obj = getattr(mod, name)
        if isinstance(obj, dg.AssetsDefinition):
            asset_keys = [key.path[-1] for key in obj.keys]
            if any(key in excluded_from_eager for key in asset_keys):
                module_assets.append(obj)
            elif not hasattr(obj, '_automation_condition') or obj._automation_condition is None:
                module_assets.append(
                    obj.with_attributes(automation_condition=dg.AutomationCondition.eager())
                )
            else:
                module_assets.append(obj)
    flattened_assets.extend(module_assets)

flattened_assets.extend(date_assets)
flattened_assets.extend(kob_assets)

automation_sensor = dg.AutomationConditionSensorDefinition(
    name="automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = Definitions(
    assets=[*flattened_assets],
    resources={
        "snowflake_io_manager": resources.snowflake_io_manager,
        "snowflake_io_manager_test": resources.snowflake_io_manager_test,
        "s3_client": resources.s3_client,
        "snowflake_resource": resources.snowflake_resource,
        "snowflake_resource_test": resources.snowflake_resource_test
    },
    sensors=[automation_sensor],
    executor=multiprocess_executor
)