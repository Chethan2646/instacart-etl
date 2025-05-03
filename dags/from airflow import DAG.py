from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import json

def send_message_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    message = {
        "order_id": 101,
        "product": "Books",
        "amount": 29.99,
        "status": "dispatched"
    }
    producer.send('orders_stream', value=message)
    producer.flush()
    producer.close()

with DAG(
    dag_id='kafka_producer_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False,
    tags=['kafka'],
) as dag:

    produce_task = PythonOperator(
        task_id='send_message',
        python_callable=send_message_to_kafka
    )

    produce_task
