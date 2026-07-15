from dagster import asset, AssetExecutionContext, AutomationCondition, EnvVar
from ..date_update_excel.daily_orderbook_script import main
from ..date_update_excel import utils as utils

DAILY_ORDERBOOK_CRON = EnvVar("DAILY_ORDERBOOK_CRON").get_value()

@asset(
    group_name="Automations",
    automation_condition=AutomationCondition.on_cron(DAILY_ORDERBOOK_CRON, 'America/New_York')
)
def daily_orderbook_asset(context: AssetExecutionContext) -> None:
    main(context, utils)
