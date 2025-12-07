from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import pandas as pd
import os


def transform_weather_data_function(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    # Parse the JSON string if it's a string
    if isinstance(data, str):
        data = json.loads(data)
    city = data['name']
    weather_description = data['weather'][0]['description']
    temperature_kelvin = data['main']['temp']   
    temperature_celsius = temperature_kelvin - 273.15
    transformed_data = {
        'city': city,
        'weather_description': weather_description,
        'temperature_celsius': round(temperature_celsius, 2)
    }   
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S")
    dt_string = 'current_weather_' + dt_string
    
    # Create the data directory if it doesn't exist
    data_dir = '/opt/airflow/dags/data/weather_data'
    os.makedirs(data_dir, exist_ok=True)
    
    csv_file_path = f'{data_dir}/{dt_string}.csv'
    df_data.to_csv(csv_file_path, index=False)
    
    # Push the file path to XCom for the next task
    task_instance.xcom_push(key='csv_file_path', value=csv_file_path)


def upload_to_s3_function(task_instance):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    # Get the file path from XCom
    csv_file_path = task_instance.xcom_pull(task_ids='transform_weather_data', key='csv_file_path')
    
    # S3 configuration
    bucket_name = 'airflow-tuple-spectra'
    s3_key = f"weather_data/{os.path.basename(csv_file_path)}"
    
    # Use Airflow's S3Hook to get credentials from connection
    s3_hook = S3Hook(aws_conn_id='aws_default_conn_id')
    s3_hook.load_file(csv_file_path, s3_key, bucket_name, replace=True)
    
    print(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")



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

# Get API key from environment variable or Airflow Variable
weather_api_key = Variable.get('WEATHER_API_KEY', default_var=os.getenv('WEATHER_API_KEY', ''))
weather_endpoint = f'data/2.5/weather?q=London,uk&APPID={weather_api_key}'

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule='0 6 * * *',  # Daily at 6 AM
    catchup=False) as dag:

    is_weather_data_available = HttpSensor(
        task_id='is_weather_api_conn_ready',
        http_conn_id='weather_api_conn',
        endpoint=weather_endpoint,
        response_check=lambda response: "weather" in response.text,
        poke_interval=5,
        timeout=20
    ) 

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weather_api_conn',
        endpoint=weather_endpoint
    )

    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data_function
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3_function
    )

    is_weather_data_available >> extract_weather_data >> transform_weather_data >> upload_to_s3