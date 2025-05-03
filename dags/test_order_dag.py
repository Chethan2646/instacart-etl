from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Function to read and count rows
def read_orders_csv():
    df = pd.read_csv('/opt/airflow/dags/orders.csv')
    row_count = len(df)
    print(f"Number of rows in orders.csv: {row_count}")

# Default arguments
default_args = {
    'owner': 'chethan',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Define the DAG
with DAG(
    dag_id='read_orders_csv_dag',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    description='Reads orders.csv and prints row count'
) as dag:

    read_task = PythonOperator(
        task_id='read_orders',
        python_callable=read_orders_csv
    )

    read_task
