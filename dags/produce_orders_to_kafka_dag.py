from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json
import random
# 🧠 Define default settings for the DAG
default_args = {
    'owner': 'chethan',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# 🧠 Define the DAG itself
with DAG(
    dag_id='produce_orders_to_kafka_dag',
    default_args=default_args,
    start_date=datetime(2024, 4, 1),
    schedule_interval=None,  # We will trigger it manually for now
    catchup=False
) as dag:

    # 🧠 Function that will run inside the PythonOperator
    def send_orders_to_kafka():
        # 🔌 Create a Kafka producer
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',  # Container name from docker-compose
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # 🧪 Sample data (could be from CSV later)
        # sample_orders = [
        #     {"order_id": 1, "customer": "Alice", "total": 25.50},
        #     {"order_id": 2, "customer": "Bob", "total": 12.00},
        #     {"order_id": 3, "customer": "Charlie", "total": 42.30},
        # ]

        

        sample_orders = [
             {"order_id": 1, "customer": "Alice", "total": 25.50, "rand": random.randint(1, 100000)},
             {"order_id": 2, "customer": "Bob", "total": 12.00, "rand": random.randint(1, 100000)},
             {"order_id": 3, "customer": "Charlie", "total": 42.30, "rand": random.randint(1, 100000)},
        ]


        # 🚀 Send each order to Kafka
        for order in sample_orders:
            producer.send('orders_stream', value=order)
            print(f"Sent order: {order}")

        # 🧼 Always flush at the end
        producer.flush()
        producer.close()

    # 🧠 Define the PythonOperator task
    produce_task = PythonOperator(
        task_id='produce_orders',
        python_callable=send_orders_to_kafka
    )

    produce_task
