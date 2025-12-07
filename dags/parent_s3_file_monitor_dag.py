from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from datetime import datetime, timedelta


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
    'parent_s3_file_monitor_dag',
    default_args=default_args,
    description='Monitor S3 for US_Cities_Sample.csv and trigger child DAG',
    schedule='*/5 * * * *',  # Check every 5 minutes
    catchup=False) as dag:

    # Wait for file to appear in S3 - uses reschedule mode so it doesn't occupy a worker
    # if file doesn't exist, it will retry according to the DAG schedule
    wait_for_s3_file = S3KeySensor(
        task_id='wait_for_s3_file',
        bucket_name='airflow-tuple-spectra',
        bucket_key='input_data/US_Cities_Sample.csv',
        aws_conn_id='aws_default_conn_id',
        poke_interval=30,  # Check every 30 seconds
        timeout=3600,  # Timeout after 1 hour
        mode='reschedule'  # Reschedule rather than poke (better for long waits)
    )

    # Trigger child DAG when file is found
    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_s3_to_redis_dag',
        trigger_dag_id='s3_to_redis_data_pipeline',
        wait_for_completion=True,
        poke_interval=10
    )

    # Move file to to_be_processed folder (prevents duplicate processing)
    move_to_processing = S3CopyObjectOperator(
        task_id='move_to_processing_folder',
        source_bucket_name='airflow-tuple-spectra',
        source_bucket_key='input_data/US_Cities_Sample.csv',
        dest_bucket_name='airflow-tuple-spectra',
        dest_bucket_key='to_be_processed/US_Cities_Sample.csv',
        aws_conn_id='aws_default_conn_id'
    )

    # Delete from input_data after moving to to_be_processed
    delete_from_input = S3DeleteObjectsOperator(
        task_id='delete_from_input_data',
        bucket='airflow-tuple-spectra',
        keys=['input_data/US_Cities_Sample.csv'],
        aws_conn_id='aws_default_conn_id'
    )

    # Move file to processed_data folder after child DAG completes
    move_to_archive = S3CopyObjectOperator(
        task_id='move_to_processed_folder',
        source_bucket_name='airflow-tuple-spectra',
        source_bucket_key='to_be_processed/US_Cities_Sample.csv',
        dest_bucket_name='airflow-tuple-spectra',
        dest_bucket_key='processed_data/US_Cities_Sample.csv',
        aws_conn_id='aws_default_conn_id'
    )

    # Delete from to_be_processed after archiving
    delete_from_processing = S3DeleteObjectsOperator(
        task_id='delete_from_processing_folder',
        bucket='airflow-tuple-spectra',
        keys=['to_be_processed/US_Cities_Sample.csv'],
        aws_conn_id='aws_default_conn_id'
    )

    wait_for_s3_file >> move_to_processing >> delete_from_input >> trigger_child_dag >> move_to_archive >> delete_from_processing
