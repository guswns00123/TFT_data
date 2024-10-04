import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
# Lambda 호출 함수 정의
def trigger_lambda(**kwargs):
    # boto3 클라이언트를 이용한 Lambda 호출
    session = boto3.Session(
        aws_access_key_id=BaseHook.get_connection('aws_lambda').login,
        aws_secret_access_key=BaseHook.get_connection('aws_lambda').password,
        region_name='ap-northeast-2'
    )
    
    client = session.client('lambda')
    response = client.invoke(
        FunctionName='TFT_data_S3',
        InvocationType='RequestResponse',  # 'Event'는 비동기 호출
        Payload=b'{"key1": "value1"}'
    )
    print(response)

# Airflow DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='dag_lambda_trigger',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Lambda 호출 작업 정의
    trigger_lambda_task = PythonOperator(
        task_id='trigger_lambda',
        python_callable=trigger_lambda,
        provide_context=True,
    )