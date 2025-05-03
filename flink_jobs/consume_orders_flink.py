from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Configuration
import snowflake.connector
import json

# 1. Inject Kafka Connector JAR
config = Configuration()
config.set_string(
    "pipeline.jars",
    "file:///opt/flink_libs/flink-sql-connector-kafka-1.17.2.jar"
)

# Snowflake credentials
sf_params = {
    "user": "COBRA02",
    "password": "Commonpw@123123",  # 🔒 Replace with actual password
    "account": "sfedu05-bvb56802",
    "warehouse": "COBRA02_WH",
    "database": "INSTACART_ETL",
    "schema": "PUBLIC",
    "role": "COBRA02_LEARNER_RL"
}

# 2. Create Execution Environment
env = StreamExecutionEnvironment.get_execution_environment(configuration=config)

# 3. Kafka Consumer Properties
properties = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'flink-consumer-group'
}

# 4. Define Kafka Consumer
kafka_consumer = FlinkKafkaConsumer(
    topics='orders_stream',
    deserialization_schema=SimpleStringSchema(),
    properties=properties
)

# 5. Read from Kafka
stream = env.add_source(kafka_consumer)

# 6. Insert Function
def process_and_insert(message):
    try:
        msg = json.loads(message)
        print(f"🔥 Flink received: {msg}")
        conn = snowflake.connector.connect(
            user=sf_params["user"],
            password=sf_params["password"],
            account=sf_params["account"],
            warehouse=sf_params["warehouse"],
            database=sf_params["database"],
            schema=sf_params["schema"],
            role=sf_params["role"]
        )
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders_stream (order_id, customer, total, rand) VALUES (%s, %s, %s, %s)",
            (msg['order_id'], msg['customer'], msg['total'], msg['rand'])
        )
        print("✅ Inserted into Snowflake")
    except Exception as e:
        print(f"❌ Error processing record: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

# 7. Map and Execute
stream.map(process_and_insert)
env.execute("Kafka to Snowflake ETL Job")
