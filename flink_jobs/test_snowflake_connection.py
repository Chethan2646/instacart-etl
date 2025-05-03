import snowflake.connector

# Replace the below with your real Snowflake credentials
conn_config = {
    'user': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'account': 'YOUR_ACCOUNT_IDENTIFIER',  # like: xy12345.us-east-1
    'warehouse': 'YOUR_WAREHOUSE',
    'database': 'YOUR_DATABASE',
    'schema': 'YOUR_SCHEMA'
}

try:
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**conn_config)
    cursor = conn.cursor()

    # Simple test query
    cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")
    result = cursor.fetchone()

    print("✅ Connection successful!")
    print(f"User: {result[0]}, Account: {result[1]}, Region: {result[2]}")

except Exception as e:
    print("❌ Connection failed:")
    print(e)

finally:
    try:
        cursor.close()
        conn.close()
    except:
        pass

# 6. Parse JSON and Insert into Snowflake
def process_and_insert(message):
    try:
        record = json.loads(message)
        print(f"🔥 Flink received: {record}")

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user='COBRA02',
            password='Commonpw@123',
            account='sfedu05-bvb56802',  # lowercase for compatibility
            warehouse='COBRA02_WH',
            database='INSTACART_ETLS',
            schema='PUBLIC'
        )

        cursor = conn.cursor()

        # Insert into table
        cursor.execute(
            """
            INSERT INTO orders_stream (order_id, customer, total, rand)
            VALUES (%s, %s, %s, %s)
            """,
            (
                int(record['order_id']),
                str(record['customer']),
                float(record['total']),
                int(record['rand'])
            )
        )

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error processing record: {e}")