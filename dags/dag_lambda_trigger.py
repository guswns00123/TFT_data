from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AWSLambdaInvokeFunctionOperator
from airflow.utils.dates import days_ago

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# DAG 정의
with DAG(
    dag_id='dag_lambda_trigger',
    default_args=default_args,
    schedule_interval=None,  # 스케줄링 필요에 따라 변경
    catchup=False
) as dag:

    # Lambda 함수를 호출하는 작업 정의
    invoke_lambda = AWSLambdaInvokeFunctionOperator(
        task_id='invoke_lambda_function',
        function_name='TFT_data_S3',  # 호출할 Lambda 함수 이름
        aws_conn_id='aws_lambda',                  # 앞서 설정한 AWS 연결 ID
        payload='{"key1": "value1", "key2": "value2"}',  # Lambda 함수에 전달할 JSON payload (선택 사항)
        log_type='Tail',                            # 로그 유형 (선택 사항)
    )

    invoke_lambda