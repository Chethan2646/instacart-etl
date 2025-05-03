from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

def load_to_postgres():
    # Read cleaned data
    df = pd.read_csv('/opt/airflow/dags/orders_cleaned.csv')

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cleaned_orders (
            order_id INT,
            user_id INT,
            eval_set TEXT,
            order_number INT,
            order_dow INT,
            order_hour_of_day INT,
            days_since_prior_order FLOAT
        );
    """)
    conn.commit()

    # Clear existing rows (optional - avoids duplicates)
    cursor.execute("DELETE FROM cleaned_orders;")
    conn.commit()

    # Insert rows
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO cleaned_orders (
                order_id, user_id, eval_set, order_number,
                order_dow, order_hour_of_day, days_since_prior_order
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'chethan',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='load_orders_to_postgres_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Load cleaned orders CSV to PostgreSQL'
) as dag:

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )
