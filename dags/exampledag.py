from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'chethan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'chethan_test_dag',
    default_args=default_args,
    description='Simple test DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello, Chethan!"',
    )

    t1
