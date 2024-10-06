import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pendulum
from airflow.exceptions import AirflowFailException
import datetime
import json

def trigger_lambda(**kwargs):
    # boto3 클라이언트를 이용한 Lambda 호출
    session = boto3.Session(
        aws_access_key_id=BaseHook.get_connection('aws_lambda').login,
        aws_secret_access_key=BaseHook.get_connection('aws_lambda').password,
        region_name='ap-northeast-2'
    )
    payload = {
        "key1": "value1",
        "key2": "value2"
    }

    payload = {
        "key1": "value1",
        "key2": "value2"
    }

    client = session.client('lambda')
    response = client.invoke(
        FunctionName='TFT_data_S3',
        InvocationType='Event',  # 비동기 호출로 변경
        Payload=json.dumps(payload).encode('utf-8')  # 페이로드 직렬화 후 전달
    )
    
    # Lambda 함수 실행을 트리거하고 즉시 반환
    status_code = response['StatusCode']
    if status_code != 202:  # 비동기 호출의 성공 응답은 202 (Accepted)
        raise AirflowFailException(f"Lambda invocation failed with status code {status_code}")
    
    print("Lambda function triggered asynchronously.")
    

def check_lambda_status(**kwargs):
    # XCom에서 Lambda 호출 결과 가져오기
    ti = kwargs['ti']
    lambda_result = ti.xcom_pull(task_ids='trigger_lambda')

    status = lambda_result['status']
    status_code = lambda_result['status_code']

    # Lambda 호출 성공 여부에 따른 처리
    if status == 'success':
        print(f"Lambda function executed successfully with status code {status_code}")
    else:
        raise AirflowFailException(f"Lambda function failed with status code {status_code}")
# def load_data_from_s3(**kwargs):
#     # S3 클라이언트 생성
#     s3_client = boto3.client('s3')

#     # S3에서 버킷과 파일 경로 지정
#     bucket_name = 'morzibucket'
#     object_key = 'path/to/your/file.csv'
    
#     # 파일을 로컬로 다운로드
#     local_file_path = '/tmp/file.csv'
    
#     # S3에서 파일 다운로드
#     s3_client.download_file(bucket_name, object_key, local_file_path)
    
#     print(f"Downloaded {object_key} from S3 bucket {bucket_name} to {local_file_path}")
with DAG(
    dag_id='dag_lambda_trigger',
    start_date=pendulum.datetime(2024,10,1, tz='Asia/Seoul'), 
    schedule='0 */6 * * *',  # 매일 6시간마다 실행
    catchup=False
) as dag:
    # Lambda 호출 작업 정의
    trigger_lambda_task = PythonOperator(
        task_id='trigger_lambda',
        python_callable=trigger_lambda,
        provide_context=True,
    )
    check_lambda_task = PythonOperator(
        task_id='check_lambda_status',
        python_callable=check_lambda_status,
        provide_context=True,
    )

    # Task 순서 정의
    trigger_lambda_task >>check_lambda_task