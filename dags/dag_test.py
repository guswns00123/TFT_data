from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from hooks.custom_postgres_hook import CustomPostgresHook
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
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
        InvocationType='Event',
        Payload=json.dumps(payload).encode('utf-8')
    )
    
    # Lambda 함수 실행을 트리거하고 즉시 반환
    status_code = response['StatusCode']
    if status_code != 202:  # 비동기 호출의 성공 응답은 202 (Accepted)
        raise AirflowFailException(f"Lambda invocation failed with status code {status_code}")
    
    print(f"Lambda function triggered for file: {file_name} asynchronously.")

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
        dag_id='dags_game_info_to_LocalDB',
        start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
        schedule='0 */3 * * *',
        catchup=False
) as dag:

    
    start = EmptyOperator(
    task_id='start'
    )
    def list_files_in_s3(bucket_name, prefix, **kwargs):
        s3_hook = S3Hook('aws_default')
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        return files

    # list_files_task = PythonOperator(
    #     task_id='list_files',
    #     python_callable=list_files_in_s3,
    #     op_kwargs={
    #         'bucket_name': 'morzibucket',
    #         'prefix': 'files/challenger_user_info_batch_'  # 파일 prefix 설정
    #     },
    #     provide_context=True,
    # )
    list_files_task = PythonOperator(
        task_id='list_s3_files',
        python_callable=list_files_in_s3,
        op_kwargs={'bucket_name': 'morzibucket', 'prefix': 'files/challenger_user_info_batch_'},  # S3 버킷 이름과 접두사 지정
        do_xcom_push=True  # 결과를 XCom에 푸시
    )

    # Lambda 호출 태스크 생성 (Dynamic Task Mapping)
    trigger_lambda_task = PythonOperator.partial(
        task_id='trigger_lambda',
        python_callable=trigger_lambda
    ).expand(
        op_kwargs={'file_name': "{{ task_instance.xcom_pull(task_ids='list_s3_files') }}"}  # XCom으로 받은 파일 목록 사용
    )

    # 태스크 의존성 설정
    list_files_task >> trigger_lambda_task