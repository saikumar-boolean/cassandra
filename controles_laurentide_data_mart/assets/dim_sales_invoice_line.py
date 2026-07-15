from dagster import asset, AssetExecutionContext, EnvVar
from .utils import warehouse_to_mart

@asset(
    group_name="Sales_Invoice_Line",
    required_resource_keys={"snowflake_resource"},
    deps=["sales_invoice_line"]
)
def dim_sales_invoice_line(context: AssetExecutionContext) -> None:
    datawarehouse = EnvVar("SNOWFLAKE_DATABASE").get_value()
    datamart = EnvVar("SNOWFLAKE_DATAMART").get_value()
    schema = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()

    warehouse_to_mart(context=context, query=f"""CREATE OR REPLACE TABLE {datamart}.{schema}.DIM_SALES_INVOICE_LINE AS
                                                    SELECT
                                                        SK_SALES_INVOICE_LINE_ID AS SK_SALES_INVOICE_LINE_ID,
                                                        COMPANY AS COMPANY,
                                                        CUSTOMER_NO AS CUSTOMER_NO,
                                                        party_type_invoice_tab AS PARTY_TYPE,
                                                        INVOICE_ID AS INVOICE_ID,
                                                        ITEM_ID AS ITEM_ID,
                                                        ITEM_DATA AS ITEM_DATA,
                                                        party_type_invoice_tab AS CREATOR,
                                                        SELLING_TOTAL_AMOUNT AS SELLING_TOTAL_AMOUNT,
                                                        SELLING_AMOUNT AS SELLING_AMOUNT,
                                                        REFERENCE AS REFERENCE,
                                                        ORDER_NO AS ORDER_NO,
                                                        LINE_NO AS LINE_NO,
                                                        RELEASE_NO AS RELEASE_NO,
                                                        CATALOG_NO AS CATALOG_NO,
                                                        DESCRIPTION AS DESCRIPTION,
                                                        CUSTOMER_PO_NO AS CUSTOMER_PO_NO,
                                                        LINE_ITEM_NO AS LINE_ITEM_NO,
                                                        PRICE_CONV AS PRICE_CONV,
                                                        SALE_UNIT_PREICE AS SALE_UNIT_PREICE,
                                                        DISCOUNT AS DISCOUNT,
                                                        ORDER_DISCOUNT AS ORDER_DISCOUNT,
                                                        CHARGE_SEQ_NO AS CHARGE_SEQ_NO,
                                                        STAGE AS STAGE,
                                                        RMA_NO AS RMA_NO,
                                                        RMA_LINE_NO AS RMA_LINE_NO,
                                                        RMA_CHARGE_NO AS RMA_CHARGE_NO,
                                                        PART_NO AS PART_NO,
                                                        PART_DESCRIPTION AS PART_DESCRIPTION,
                                                        UNIT_OF_MEASURE AS UNIT_OF_MEASURE,
                                                        PRICE_UNIT_OF_MEASURE AS PRICE_UNIT_OF_MEASURE,
                                                        PO_REF_NUMBER AS PO_REF_NUMBER,
                                                        UNIT_PRICE_INCL_TAX AS UNIT_PRICE_INCL_TAX,
                                                        ACTUAL_NET_CURR_AMOUNT AS ACTUAL_NET_CURR_AMOUNT,
                                                        ACTUAL_NET_DOM_AMOUNT AS ACTUAL_NET_DOM_AMOUNT,
                                                        PO_LINE_NUMBER AS PO_LINE_NUMBER,
                                                        PO_RELEASE_NUMBER AS PO_RELEASE_NUMBER,
                                                        PO_RECEIPT_NUMBER AS PO_RECEIPT_NUMBER,
                                                        INV_NET_UNIT_PRICE AS INV_NET_UNIT_PRICE,
                                                        ORIGINAL_INVOICED_QTY AS ORIGINAL_INVOICED_QTY,
                                                        SERIES_ID AS SERIES_ID,
                                                        INVOICE_NO AS INVOICE_NO,
                                                        INVOICE_DATE AS INVOICE_DATE,
                                                        DUE_DATE AS DUE_DATE,
                                                        INVOICE_TYPE AS INVOICE_TYPE,
                                                        PAY_TERM_ID AS PAY_TERM_ID,
                                                        DELIVERY_DATE AS DELIVERY_DATE,
                                                        ARRIVAL_DATE AS ARRIVAL_DATE,
                                                        CREATION_DATE AS CREATION_DATE,
                                                        CURRENCY_RATE AS CURRENCY_RATE,
                                                        DIV_FACTOR AS DIV_FACTOR,
                                                        CURRENCY AS CURRENCY,
                                                        PROJECT_ID AS PROJECT_ID,
                                                        POSTED_DATE AS POSTED_DATE
                                                    FROM {datawarehouse}.{schema}.SALES_INVOICE_LINE;""")