from dagster import asset, AssetExecutionContext, AutomationCondition, EnvVar, AssetSelection
from .utils import prepare_local_parquet_paths, upload_duckdb_to_snowflake, get_prefixed_columns, add_shift_cron_time
import duckdb

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()
NEW_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=6, minutes_to_add=45)

@asset(
    name="inventory_value",
    group_name="Inventory",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "ifs_inventory_value_part_tab",
        "ifs_inventory_part_period_hist_tab",
        "ifs_invent_value_part_detail_tab"
    ],
    automation_condition=AutomationCondition.on_cron(
        NEW_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def inventory_value(context: AssetExecutionContext) -> None:
    s3 = context.resources.s3_client
    snowflake = context.resources.snowflake_resource
    parquet_sources = {
        "inventory_value_part_tab": "ifs/inventory_value_part_tab/",
        "inventory_part_period_hist_tab": "ifs/inventory_part_period_hist_tab/",
        "invent_value_part_detail_tab": "ifs/invent_value_part_detail_tab/"
    }

    local_paths, tmpdir = prepare_local_parquet_paths(context, s3, parquet_sources)

    invpart_path = local_paths["inventory_value_part_tab"]
    invhist_path = local_paths["inventory_part_period_hist_tab"]
    invdetail_path = local_paths["invent_value_part_detail_tab"]
    

    try:

        con = duckdb.connect()
        con.execute(f"SELECT * FROM '{invpart_path}' LIMIT 1")
        con.execute(f"SELECT * FROM '{invhist_path}' LIMIT 1")
        con.execute(f"SELECT * FROM '{invdetail_path}' LIMIT 1")

        select_clause = ",\n  ".join(
            get_prefixed_columns(con, invpart_path, "inventory_value_part_tab") +
            get_prefixed_columns(con, invhist_path, "inventory_part_period_hist_tab") +
            get_prefixed_columns(con, invdetail_path, "invent_value_part_detail_tab")
        )

        context.log.info(f"Select Statement: {select_clause}")

        duckdb_sql = f"""
        SELECT 
        {select_clause}
        FROM '{invpart_path}' AS inventory_value_part_tab
        LEFT JOIN '{invhist_path}' AS inventory_part_period_hist_tab
        ON inventory_value_part_tab.CONTRACT = inventory_part_period_hist_tab.CONTRACT
        AND inventory_value_part_tab.STAT_YEAR_NO = inventory_part_period_hist_tab.STAT_YEAR_NO
        AND inventory_value_part_tab.STAT_PERIOD_NO = inventory_part_period_hist_tab.STAT_PERIOD_NO
        AND inventory_value_part_tab.PART_NO = inventory_part_period_hist_tab.PART_NO
        LEFT JOIN '{invdetail_path}' AS invent_value_part_detail_tab
        ON inventory_value_part_tab.CONTRACT = invent_value_part_detail_tab.CONTRACT
        AND inventory_value_part_tab.STAT_YEAR_NO = invent_value_part_detail_tab.STAT_YEAR_NO
        AND inventory_value_part_tab.STAT_PERIOD_NO = invent_value_part_detail_tab.STAT_PERIOD_NO
        AND inventory_value_part_tab.PART_NO = invent_value_part_detail_tab.PART_NO
        """

        # Finally run the query
        relation = con.from_query(duckdb_sql)

        upload_duckdb_to_snowflake(
            context=context,
            duckdb_relation=relation,
            snowflake_resource=snowflake,
            final_table_name="inventory_value"
            )
    finally:
        context.log.info("Cleaning up DuckDB and temp parquet files")
        con.close()
        tmpdir.cleanup()