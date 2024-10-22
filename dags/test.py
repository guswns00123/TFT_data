from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
import csv

def process_user_data(postgres_conn_id, query, batch_size=300, file_prefix=None, **kwargs):
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
    cursor = conn.cursor()

    offset = 0
    batch_number = 1
    while True:
        # LIMIT과 OFFSET을 사용해 배치 처리
        batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
        cursor.execute(batch_query)
        batch_data = cursor.fetchall()

        if not batch_data:
            break  # 더 이상 데이터가 없으면 루프 종료

        # 배치 데이터를 CSV로 저장
        file_path = f"{file_prefix}{batch_number}.csv"
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(batch_data)  # 데이터 저장

        # 다음 배치로 넘어감
        offset += batch_size
        batch_number += 1

    cursor.close()
    conn.close()

with DAG(
        dag_id='test',
        start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
        schedule='0 1 * * *', # 매일 새벽 1시
        catchup=False
) as dag:
    

    start = EmptyOperator(
    task_id='start'
    )

    process_user = PythonOperator(
    task_id="process_user_data",
    python_callable=process_user_data,
    op_kwargs={
        'postgres_conn_id': 'conn-db-postgres-custom',
        'query': 'SELECT * FROM user_info',
        'batch_size': 200,  # 한 번에 처리할 배치 크기 설정
        'file_prefix': '/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/batch_user_data_'  # 저장 파일 경로 설정
    },
    provide_context=True
)