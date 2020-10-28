import snowflake.connector

def get_snowflake_connection(usr, pwd, role, warehouse_name, db_name=None):
        
    if db_name is None:
        db_name = 'PC_STITCH_DB'
        
    conn = snowflake.connector.connect(user=usr, 
                           password=pwd,
                           account='gl11689.us-east-1',
                           warehouse=warehouse_name,
                           database=db_name)

    conn.cursor().execute("USE role {}".format(role))
    
    return conn