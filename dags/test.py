from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
import csv
import os
import psutil
import logging
def measure_memory_usage(task_id):
    process = psutil.Process()
    mem_info = process.memory_info()
    logging.info(f'Task ID: {task_id} - Memory Usage: {mem_info.rss / (1024 * 1024):.2f} MB')  # RSS (Resident Set Size)

def process_user_data(postgres_conn_id, query, batch_size=300, file_prefix=None, **kwargs):
    import time
    measure_memory_usage('process_user_data')

    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
    cursor = conn.cursor()

    offset = 0
    batch_number = 1
    directory = os.path.dirname(file_prefix)
    start_time = time.time()  # 시작 시간 기록
    process = psutil.Process()  # 현재 프로세스 정보 가져오기
    initial_memory = process.memory_info().rss / (1024 ** 2)

    # 디렉토리 생성
    os.makedirs(directory, exist_ok=True)

    while True:
        # LIMIT과 OFFSET을 사용해 배치 처리
        batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
        cursor.execute(batch_query)
        batch_data = cursor.fetchall()

        if not batch_data:
            break  # 더 이상 데이터가 없으면 루프 종료

        # 파일 이름을 배치 번호에 따라 지정
        file_name = f"batch_user_data_{batch_number}.csv"
        file_path = os.path.join(directory, file_name)  # 전체 파일 경로 생성

        # 배치 데이터를 CSV로 저장
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(batch_data)  # 데이터 저장

        # 다음 배치로 넘어감
        offset += batch_size
        batch_number += 1
    end_time = time.time()  # 종료 시간 기록
    final_memory = process.memory_info().rss / (1024 ** 2)  # 최종 메모리 사용량 (MB 단위)

    execution_time = end_time - start_time  # 실행 시간 계산
    memory_usage = final_memory - initial_memory
    cursor.close()
    conn.close()
    print(f"Batch Processing Execution Time: {execution_time} seconds")
    print(f"Batch Processing Memory Usage: {memory_usage} MB")

def process_user_data2(postgres_conn_id, query, file_prefix=None, **kwargs):
    import time
    measure_memory_usage('process_user_data2')
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
    cursor = conn.cursor()
    directory = os.path.dirname(file_prefix)
    start_time = time.time()  # 시작 시간 기록
    process = psutil.Process()  # 현재 프로세스 정보 가져오기
    initial_memory = process.memory_info().rss / (1024 ** 2)

    # 디렉토리 생성
    os.makedirs(directory, exist_ok=True)

    batch_query = f"{query}"
    cursor.execute(batch_query)
    batch_data = cursor.fetchall()

    file_name = f"batch_user_data_1.csv"
    file_path = os.path.join(directory, file_name) 

    # 배치 데이터를 CSV로 저장
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(batch_data) 
    end_time = time.time()  # 종료 시간 기록
    final_memory = process.memory_info().rss / (1024 ** 2)  # 최종 메모리 사용량 (MB 단위)

    execution_time = end_time - start_time  # 실행 시간 계산
    memory_usage = final_memory - initial_memory
    cursor.close()
    conn.close()
    print(f"Batch Processing Execution Time: {execution_time} seconds")
    print(f"Batch Processing Memory Usage: {memory_usage} MB")



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
        'file_prefix': '/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/'  # 저장 파일 경로 설정
    },
    provide_context=True
)
    process_user2 = PythonOperator(
        task_id="process_user_data2",
        python_callable=process_user_data2,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'query': 'SELECT * FROM user_info', 
            'file_prefix': '/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/'
        },
        provide_context=True
    )
    start >> process_user >>process_user2