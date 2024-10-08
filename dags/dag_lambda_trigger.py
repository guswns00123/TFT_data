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

def trigger_lambda(file_name,**kwargs):
    # boto3 클라이언트를 이용한 Lambda 호출
    session = boto3.Session(
        aws_access_key_id=BaseHook.get_connection('aws_lambda').login,
        aws_secret_access_key=BaseHook.get_connection('aws_lambda').password,
        region_name='ap-northeast-2'
    )
    payload = {
        "file_name": file_name  # 파일 이름을 페이로드로 전달
    }

    client = session.client('lambda')
    response = client.invoke(
        FunctionName='TFT_data_S3',
        InvocationType='RequestResponse',  # 동기 호출로 변경
        Payload=json.dumps(payload).encode('utf-8')
    )
    
    # Lambda 함수 실행 결과 가져오기
    response_payload = response['Payload'].read().decode('utf-8')
    status_code = response['StatusCode']

    if status_code == 200:  # 동기 호출의 성공 응답은 200
        print(f"Lambda function executed successfully for file: {file_name}")
        print(f"Response: {response_payload}")
    else:
        raise AirflowFailException(f"Lambda invocation failed with status code {status_code}")
    print(f"Lambda function triggered for file: {response} asynchronously.")

# def trigger_lambda2(file_name,**kwargs):
#     session = boto3.Session(
#         aws_access_key_id=BaseHook.get_connection('aws_lambda').login,
#         aws_secret_access_key=BaseHook.get_connection('aws_lambda').password,
#         region_name='ap-northeast-2'
#     )

#     client = session.client('lambda')
#     response = client.invoke(
#         FunctionName='get_game_result',
#         InvocationType='Event',
#         Payload=json.dumps(response).encode('utf-8')
#     )
    
#     # Lambda 함수 실행을 트리거하고 즉시 반환
#     status_code = response['StatusCode']
#     if status_code != 202:  # 비동기 호출의 성공 응답은 202 (Accepted)
#         raise AirflowFailException(f"Lambda invocation failed with status code {status_code}")
    
#     print(f"Lambda function triggered for file: {file_name} asynchronously.")
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

    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files_in_s3,
        op_kwargs={
            'bucket_name': 'morzibucket',
            'prefix': 'files/challenger_user_info_batch_'  # 파일 prefix 설정
        },
        provide_context=True,
    )
    def create_lambda_tasks_from_list(**kwargs):
        ti = kwargs['ti']
        file_names = ti.xcom_pull(task_ids='list_files')
        response_list = []

        for file_name in file_names:
            trigger_lambda(file_name)  # 각 파일에 대해 Lambda 호출
            

        return response_list  # 모든 응답을 반환

    create_lambda_tasks_op = PythonOperator(
        task_id='create_lambda_tasks',
        python_callable=create_lambda_tasks_from_list,
        provide_context=True,
    )



    list_files_task >> create_lambda_tasks_op 