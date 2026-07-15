import pandas as pd
from dagster import asset, AssetExecutionContext, EnvVar, AutomationCondition, AssetSelection
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df, add_shift_cron_time

PIPELINES_CRON = EnvVar("PIPELINES_CRON").get_value()
NEW_CRON = add_shift_cron_time(PIPELINES_CRON, hours_to_add=7, minutes_to_add=0)

@asset(
    name="sales_part",
    group_name="Inventory",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    tags={"tblinvoiced": ""},
    deps=[
        "ifs_sales_part_tab",
        "ifs_sales_part_cft",
        "ifs_sales_part_cross_reference_tab",
        "ifs_sales_part_language_desc_tab",
        "ifs_sales_part_base_price_tab",
        "ifs_sales_price_list_part_tab"
    ],
    automation_condition=AutomationCondition.on_cron(
        NEW_CRON, 
        "America/New_York"
    ).ignore(AssetSelection.all())
)
def sales_part(context: AssetExecutionContext) -> pd.DataFrame:
    s3 = context.resources.s3_client

    spt = generic_extraction_and_cleaning(context, "ifs/sales_part_tab/", s3, 
                                          columns_to_drop=['eng_attribute', 'customs_stat_no', 'discount_group', 
                                                           'print_control_code', 'package_type', 'volume', 
                                                           'weight_net', 'tax_code', 'tax_class_id', 
                                                           'proposed_parcel_qty', 'cust_warranty_id', 
                                                           'intrastat_conv_factor', 'rule_id', 'pallet_type', 
                                                           'proposed_pallet_qty', 'delivery_type', 'country_of_origin', 
                                                           'statistical_code', 'acquisition_origin', 'acquisition_reason_id', 
                                                           'hsn_sac_code', 'saft_category'])
    spcft = generic_extraction_and_cleaning(context, "ifs/sales_part_cft/", s3, columns_to_drop=[])
    spxref = generic_extraction_and_cleaning(context, "ifs/sales_part_cross_reference_tab/", s3, columns_to_drop=['min_durab_days_co_deliv'])
    spld = generic_extraction_and_cleaning(context, "ifs/sales_part_language_desc_tab/", s3, columns_to_drop=[])
    spbp = generic_extraction_and_cleaning(context, "ifs/sales_part_base_price_tab/", s3, columns_to_drop=['cost_set', 'template_id'])
    splp = generic_extraction_and_cleaning(context, "ifs/sales_price_list_part_tab/", s3, 
                                           columns_to_drop=['rounding', 'discount_type', 'discount', 
                                                            'price_break_template_id', 'valid_to_date'])

    df = spt \
        .merge(spcft, on="rowkey", how="left", suffixes=("", "_cft")) \
        .merge(spxref, on=["catalog_no", "contract"], how="left", suffixes=("", "_xref")) \
        .merge(spld, on=["catalog_no", "contract"], how="left", suffixes=("", "_lang")) \
        .merge(spbp, on="catalog_no", how="left", suffixes=("", "_base")) \
        .merge(splp, on=["catalog_no", "base_price_site"], how="left", suffixes=("", "_pricelist"))

    ensure_snowflake_schema_matches_df(context, df)

    return df
