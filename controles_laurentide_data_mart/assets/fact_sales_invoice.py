from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Sales_Invoice_Line",
    required_resource_keys={"snowflake_resource"},
    deps=["sales_invoice_line", "customer", "part"]
)
def fact_sales_invoice(context: AssetExecutionContext) -> None:
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()
    datamart = EnvVar("SNOWFLAKE_DATAMART").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()

    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.FACT_SALES_INVOICE AS
                                                    (SELECT
                                                        'CUSTOMER INVOICE'         AS INVOICE_TYPE,
                                                        C.SK_CUSTOMER_ID            AS CUSTOMER_ID, 
                                                        P.SK_PART_ID                AS PART_ID, 
                                                        SIL.INVOICE_DATE            AS INVOICE_DATE_ID, 
                                                        SIL.INVOICED_QTY           AS INVOICED_QUANTITY, 
                                                        COALESCE(SIL.SELLING_AMOUNT / NULLIF(SIL.INVOICED_QTY, 0), 0) AS LINE_UNIT_PRICE_CAD, 
                                                        SIL.SELLING_AMOUNT         AS LINE_NET_AMOUNT_CAD, 
                                                        COALESCE(SIL.VAT_DOM_AMOUNT, 0)                      AS LINE_TAX_AMOUNT_CAD, 
                                                        COALESCE(SIL.SELLING_AMOUNT + SIL.VAT_DOM_AMOUNT, 0)  AS LINE_GROSS_AMOUNT_CAD,
                                                        SIL.DISCOUNT               AS LINE_DISCOUNT_PERCENTAGE
                                                    FROM {datawarehouse}.{schema}.SALES_INVOICE_LINE SIL 
                                                    INNER JOIN {datawarehouse}.{schema}.CUSTOMER C ON SIL.CUSTOMER_NO = C.CUSTOMER_NO 
                                                    INNER JOIN {datawarehouse}.{schema}.PART P ON SIL.CATALOG_NO = P.PART_NO
                                                    WHERE SIL.series_id <> 'PR' 
                                                    AND SIL.catalog_no NOT IN ('FEE-EXP', 'HEE-HDL', 'FEE-TRANS', 'FEE_ADM')
                                                    AND SIL.party_type_invoice_item_tab = 'CUSTOMER'
                                                    AND SIL.party_type_invoice_tab = 'CUSTOMER'
                                                    AND SIL.creator_invoice_item_tab = 'CUSTOMER_ORDER_INV_ITEM_API'
                                                    AND SIL.creator_invoice_tab = 'CUSTOMER_ORDER_INV_HEAD_API' 
                                                    AND SIL.rowstate != 'Cancelled')
                                                    UNION ALL
                                                    (Select
                                                        'PROJECT INVOICE'          AS INVOICE_TYPE,
                                                        NULL                        AS CUSTOMER_ID, 
                                                        NULL                        AS PART_ID, 
                                                        SIL.INVOICE_DATE            AS INVOICE_DATE_ID, 
                                                        SIL.INVOICED_QTY           AS INVOICED_QUANTITY, 
                                                        COALESCE(SIL.SELLING_AMOUNT / NULLIF(SIL.INVOICED_QTY, 0), 0) AS LINE_UNIT_PRICE_CAD, 
                                                        SIL.SELLING_AMOUNT         AS LINE_NET_AMOUNT_CAD, 
                                                        COALESCE(SIL.VAT_DOM_AMOUNT, 0)                      AS LINE_TAX_AMOUNT_CAD, 
                                                        COALESCE(SIL.SELLING_AMOUNT + SIL.VAT_DOM_AMOUNT, 0)  AS LINE_GROSS_AMOUNT_CAD,
                                                        SIL.DISCOUNT               AS LINE_DISCOUNT_PERCENTAGE
                                                    FROM {datawarehouse}.{schema}.SALES_INVOICE_LINE SIL 
                                                    WHERE party_type_invoice_item_tab = 'CUSTOMER'
                                                    AND party_type_invoice_tab = 'CUSTOMER'
                                                    AND series_id <> 'PR'
                                                    AND creator_invoice_item_tab = 'PROJECT_INVOICE_ITEM_API');""")
                                                    