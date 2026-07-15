from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Purchase",
    required_resource_keys={"snowflake_resource"},
    deps=["payable_transaction", "purchase_transaction", "inventory_transaction"]
)
def fact_cost_transaction(context: AssetExecutionContext) -> None:
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE_TEST").get_value()
    datamart = EnvVar("SNOWFLAKE_DATAMART_TEST").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()

    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.FACT_COST_TRANSACTION AS
                                                    (SELECT
                                                        COST                        AS COST_AMOUNT_CAD,
                                                        NULL                        AS COST_AMOUNT_USD,
                                                        TRANSACTION_DATE            AS TRANSACTION_DATE_ID,
                                                        ROW_ID                      AS TRANSACTION_ID,
                                                        PART_NO                     AS PART_ID,
                                                        CASE
                                                            WHEN DEMAND_CODE in ('Cust Order Dir', 'Cust Order Trans') THEN 'COST - Customer Order Supp Inv Transactions'
                                                            WHEN DEMAND_CODE = 'Project' THEN 'COST - Project Supp Inv Transactions'
                                                            WHEN DEMAND_CODE = 'Work Order' THEN 'COST - Work Order Supp Inv Transactions'
                                                            WHEN DEMAND_CODE = 'Non Inventory' OR DEMAND_CODE IS NULL THEN 'COST - Non-Inventory Supp Inv Trans'
                                                            ELSE NULL
                                                        END AS TYPE
                                                    FROM {datawarehouse}.{schema}.PAYABLE_TRANSACTION)
                                                    UNION ALL
                                                    (SELECT
                                                        COST                        AS COST_AMOUNT_CAD,
                                                        NULL                        AS COST_AMOUNT_USD,
                                                        TRANSACTION_DATE            AS TRANSACTION_DATE_ID,
                                                        TRANSACTION_ID              AS TRANSACTION_ID,
                                                        PART_NO                     AS PART_ID,
                                                        CASE
                                                            WHEN ORDER_TYPE_REF = 'CUST ORDER' THEN 'COST - Customer Order Inventory Transactions'
                                                            WHEN ORDER_TYPE_REF = 'RMA' THEN 'COST - RMA Inventory Transactions'
                                                            WHEN ORDER_TYPE_REF = 'PROJECT' THEN  'COST - Project Inventory Transactions'
                                                            ELSE NULL
                                                        END AS TYPE
                                                    FROM {datawarehouse}.{schema}.INVENTORY_TRANSACTION)                                                    
                                                    UNION ALL
                                                    (SELECT
                                                        COST                        AS COST_AMOUNT_CAD,
                                                        NULL                        AS COST_AMOUNT_USD,
                                                        TRANSACTION_DATE            AS TRANSACTION_DATE_ID,
                                                        NULL                        AS TRANSACTION_ID,
                                                        PART_NO                     AS PART_ID,
                                                        CASE
                                                            WHEN DEMAND_CODE in ('Cust Order Dir', 'Cust Order Trans') THEN 'COST - Customer Order PO Transactions'
                                                            WHEN DEMAND_CODE in ('Project','Project Inventory') THEN 'COST - Project Supp Inv Transactions'
                                                            WHEN DEMAND_CODE = 'Work Order' THEN  'COST - Work Order PO Transactions'
                                                            WHEN DEMAND_CODE = 'Non Inventory' OR DEMAND_CODE IS NULL THEN 'COST - Non-Inventory PO Trans'
                                                            ELSE NULL
                                                        END AS TYPE
                                                    FROM {datawarehouse}.{schema}.PURCHASE_TRANSACTION);""")