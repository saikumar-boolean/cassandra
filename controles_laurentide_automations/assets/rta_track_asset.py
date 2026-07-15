from dagster import asset, AssetExecutionContext, AutomationCondition, EnvVar
from ..date_update_excel.rta_track_script import main
from ..date_update_excel import utils as utils

RTA_TRACK_CRON = EnvVar("RTA_TRACK_CRON").get_value()

@asset(
    group_name="Automations",
    automation_condition=AutomationCondition.on_cron(RTA_TRACK_CRON, 'America/New_York')
)
def rta_track_asset(context: AssetExecutionContext) -> None:
    main(context, utils)
