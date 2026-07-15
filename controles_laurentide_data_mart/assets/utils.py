def warehouse_to_mart(context, query):
    try:    
        context.resources.snowflake_resource.execute_query(query)
        context.log.info("Table Created")
    except Exception as e:
        context.log.error("Error in Snowflake connection or SQL query")
        context.log.error("Error msg: " + str(e))
        raise