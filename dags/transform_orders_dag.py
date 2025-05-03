from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def clean_orders_csv():
    df = pd.read_csv('/opt/airflow/dags/orders.csv')
    
    # Drop missing values
    df.dropna(inplace=True)

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Save cleaned file
    df.to_csv('/opt/airflow/dags/orders_cleaned.csv', index=False)
    print(f"Cleaned data saved. Rows remaining: {len(df)}")

default_args = {
    'owner': 'chethan',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='clean_orders_csv_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Clean orders.csv and save cleaned version'
) as dag:
    
    clean_task = PythonOperator(
        task_id='clean_orders',
        python_callable=clean_orders_csv
    )

    clean_task
