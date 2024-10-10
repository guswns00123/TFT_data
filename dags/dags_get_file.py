import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pendulum
from io import StringIO

def download_file_from_s3(bucket_name, file_name, local_path, **kwargs):
    s3_hook = S3Hook('aws_default')
    s3_client = s3_hook.get_conn()

    # Download the file from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    content = obj['Body'].read().decode('utf-8')

    # Convert the file content to a DataFrame (assuming it's a CSV file)
    df = pd.read_csv(StringIO(content))
    
    # Save to local directory
    local_file_path = f"{local_path}/{file_name.split('/')[-1]}"
    df.to_csv(local_file_path, index=False)

    print(f"File saved locally at {local_file_path}")
    return local_file_path

with DAG(
    dag_id='dags_get_file',
    start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'), 
    schedule_interval='0 * * * *',  # Every 6 hours
    catchup=False
) as dag:

    
    # Download the specific file from S3
    download_file_task = PythonOperator(
    task_id='download_file',
    python_callable=download_file_from_s3,
    op_kwargs={
        'bucket_name': 'morzibucket',
        'file_name': 'game_res/challenger_game_id_1.csv',
        'local_path': '/opt/airflow/files/game_id'  # Local path to save the file
    },
    provide_context=True,
)

download_file_task