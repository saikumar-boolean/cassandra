from dagster import asset, AssetExecutionContext, AutomationCondition, EnvVar
from ..date_update_excel.impact_partner_script import main
from ..date_update_excel import utils as utils

IMPACT_PARTNER_CRON = EnvVar("IMPACT_PARTNER_CRON").get_value()

@asset(
    group_name="Automations",
    automation_condition=AutomationCondition.on_cron(IMPACT_PARTNER_CRON, 'America/New_York')
)
def impact_partner_asset(context: AssetExecutionContext) -> None:
    main(context, utils)
