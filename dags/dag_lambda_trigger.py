import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pendulum
from airflow.exceptions import AirflowFailException
import datetime
import json
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task


@task
def list_files_in_s3(bucket_name, prefix):
    s3_hook = S3Hook('aws_default')
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    return files

@task
def trigger_lambda_batch(file_names):
    session = boto3.Session(
        aws_access_key_id=BaseHook.get_connection('aws_lambda').login,
        aws_secret_access_key=BaseHook.get_connection('aws_lambda').password,
        region_name='ap-northeast-2'
    )
    client = session.client('lambda')

    for file_name in file_names:
        payload = {"file_name": file_name}
        response = client.invoke(
            FunctionName='TFT_data_S3',
            InvocationType='Event',
            Payload=json.dumps(payload).encode('utf-8')
        )
        status_code = response['StatusCode']
        if status_code != 202:
            raise AirflowFailException(f"Lambda invocation failed for {file_name}")
        print(f"Triggered Lambda for {file_name}")

with DAG(
    dag_id='dag_lambda_trigger',
    start_date=pendulum.datetime(2024,10,1, tz='Asia/Seoul'), 
    schedule='0 * * * *',  # 매일 6시간마다 실행
    catchup=False
) as dag:
    def list_files_in_s3(bucket_name, prefix, **kwargs):
        s3_hook = S3Hook('aws_default')
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        return files

    list_files_task = list_files_in_s3(
        bucket_name='morzibucket',
        prefix='files/challenger_user_info_batch_'
    )

    # Step 2: 동적으로 3개의 파일씩 Lambda 호출
    trigger_lambdas_task = trigger_lambda_batch.expand(
        file_names=list_files_task[:3]  # 3개씩 그룹화하여 실행
    )

    list_files_task >> trigger_lambdas_task

