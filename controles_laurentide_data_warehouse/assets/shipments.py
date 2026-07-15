import pandas as pd
from dagster import asset, AssetExecutionContext
from .utils import generic_extraction_and_cleaning, ensure_snowflake_schema_matches_df

@asset(
    group_name="Shipments",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_shipment_tab"]
)
def shipment(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shipment_tab/",
                                           columns_to_drop = [
    "sender_county", "forward_agent_id", "airway_bill_no", "sender_reference", "place_of_departure",
    "place_of_destination", "remit_cod_to", "freight_payer_note", "qty_eur_pallets", "fix_deliv_freight",
    "freight_map_id", "zone_id", "price_list_no", "parent_consol_shipment_id", "route_id", "ref_id",
    "dock_code", "sub_dock_code", "planned_ship_period", "load_sequence_no", "manual_volume",
    "transport_unit_type", "volume_capacity", "weight_capacity", "approved_by", "customs_value_currency",
    "shipment_freight_payer_id", "sender_address3", "sender_address4", "sender_address5", "sender_address6",
    "receiver_address3", "receiver_address4", "receiver_address5", "receiver_address6", "packing_proposal_id"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shipments",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_shipment_freight_tab"]
)
def shipment_freight(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shipment_freight_tab/",
                                           columns_to_drop = ["fix_deliv_freight", "freight_map_id", 
                                                              "zone_id", "price_list_no"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shipments",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_shipment_in_tab"]
)
def shipment_in(context: AssetExecutionContext) -> pd.DataFrame:
    merged_df = generic_extraction_and_cleaning(context=context, 
                                           s3=context.resources.s3_client, 
                                           key="ifs/shipment_in_tab/",
                                           columns_to_drop = ["handling_charge_type", 
                                                              "ship_via_code", "line_no", "rel_no"])
    ensure_snowflake_schema_matches_df(context, merged_df)
    return merged_df

@asset(
    group_name="Shipments",
    io_manager_key="snowflake_io_manager",
    required_resource_keys={"s3_client", "snowflake_resource"},
    deps=["ifs_transport_task_tab",
          "ifs_transport_task_cft"]
)

def transport_task(context: AssetExecutionContext) -> pd.DataFrame:

    retrieve_from_s3_transport_task_tab = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/transport_task_tab/")

    retrieve_from_s3_transport_task_cft = generic_extraction_and_cleaning(
        context=context, 
        s3=context.resources.s3_client, 
        key="ifs/transport_task_cft/",
        columns_to_drop = ["cf_coordinator"])

    df = pd.merge(
        retrieve_from_s3_transport_task_tab, 
        retrieve_from_s3_transport_task_cft,
        on="rowkey", how="left", 
        suffixes=("_transport_task_tab", "_transport_task_cft"))
    
    ensure_snowflake_schema_matches_df(context, df)

    return df