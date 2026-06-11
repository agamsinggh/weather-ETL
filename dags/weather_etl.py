from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

# --- EXTRACT ---
def extract():
    url = "https://wttr.in/Delhi?format=j1"
    response = requests.get(url)
    data = response.json()
    temp = data['current_condition'][0]['temp_C']
    print(f"Extracted temp: {temp}°C")
    return temp

# --- TRANSFORM ---
def transform(**context):
    temp = context['ti'].xcom_pull(task_ids='extract_task')
    temp_f = (int(temp) * 9/5) + 32
    print(f"Transformed: {temp}°C → {temp_f}°F")
    return {"celsius": temp, "fahrenheit": temp_f}

# --- LOAD ---
def load(**context):
    data = context['ti'].xcom_pull(task_ids='transform_task')
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # Create table if it doesn't exist
    hook.run("""
        CREATE TABLE IF NOT EXISTS weather (
            id SERIAL PRIMARY KEY,
            celsius FLOAT,
            fahrenheit FLOAT,
            recorded_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Insert the data
    hook.run("""
        INSERT INTO weather (celsius, fahrenheit)
        VALUES (%s, %s);
    """, parameters=[data['celsius'], data['fahrenheit']])

    print(f"Successfully saved to database: {data}")

# --- PIPELINE ---
with DAG(
    dag_id='weather_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    extract_task >> transform_task >> load_task