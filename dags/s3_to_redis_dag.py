from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import pandas as pd
import os


def create_redis_table_func(**context):
    """Create redis data table using PostgresHook"""
    pg_hook = PostgresHook(postgres_conn_id='redis_postgres_conn_id')
    sql = '''CREATE TABLE IF NOT EXISTS redis_data (
        City TEXT NOT NULL,	
        State TEXT NOT NULL,	
        Zip NUMERIC NOT NULL,
        Census_2020 NUMERIC NOT NULL
    );'''
    pg_hook.run(sql)
    print("Created redis_data table successfully")


def truncate_redis_table_func(**context):
    """Truncate redis data table using PostgresHook"""
    pg_hook = PostgresHook(postgres_conn_id='redis_postgres_conn_id')
    sql = 'TRUNCATE TABLE redis_data;'
    pg_hook.run(sql)
    print("Truncated redis_data table successfully")


def load_csv_to_redis_func(**context):
    """Read CSV from S3 and load data into redis_data table"""
    import io
    
    # Get S3 file from to_be_processed folder (moved there by parent DAG)
    s3_hook = S3Hook(aws_conn_id='aws_default_conn_id')
    file_content = s3_hook.read_key(
        key='to_be_processed/US_Cities_Sample.csv',
        bucket_name='airflow-tuple-spectra'
    )
    
    # Read CSV into pandas DataFrame
    df = pd.read_csv(io.StringIO(file_content))
    print(f"Read {len(df)} rows from S3 CSV")
    
    # Insert data into Redis table
    pg_hook = PostgresHook(postgres_conn_id='redis_postgres_conn_id')
    
    for index, row in df.iterrows():
        sql = f"""
        INSERT INTO redis_data (City, State, Zip, Census_2020)
        VALUES ('{row['City']}', '{row['State']}', {row['Zip']}, {row['Census_2020']});
        """
        pg_hook.run(sql)
    
    print(f"Successfully loaded {len(df)} rows into redis_data table")






default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,  
    'email': ['harish.techlance@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    's3_to_redis_data_pipeline',
    default_args=default_args,
    description='A simple redis data pipeline',
    schedule=None,  # Triggered by parent DAG only
    catchup=False) as dag:

    start_pipeline = PythonOperator(
        task_id='start_pipeline',
        python_callable=lambda: print("Starting S3 to Redis pipeline...")
    )

    with TaskGroup(group_id='s3_to_redis_group', tooltip='Tasks for transferring data from S3 to Redis') as s3_to_redis_group:
        create_redis_table = PythonOperator(
            task_id='create_cities_redis_table',
            python_callable=create_redis_table_func
        )

        load_csv_task = PythonOperator(
            task_id='load_csv_to_redis_table',
            python_callable=load_csv_to_redis_func
        )

        create_redis_table >> load_csv_task

    start_pipeline >> s3_to_redis_group