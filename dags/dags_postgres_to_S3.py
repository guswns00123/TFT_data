from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd

with DAG(
        dag_id='dags_postgres_to_S3',
        start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    '''Postgres DB 정보를 Airflow를 통하여 S3에 적재'''
    start = EmptyOperator(
    task_id='start'
    )

    def process_user_data(postgres_conn_id, query, **kwargs):

        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        
        # 쿼리 실행 후 결과 가져오기
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        
        # 데이터를 CSV로 저장
        df = pd.DataFrame(result)
        
        return df

    process_user = PythonOperator(
        task_id="process_user_data",
        python_callable=process_user_data,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'query': 'SELECT * FROM tft_user_info',  # 실행할 쿼리
        },
        provide_context=True
    )
 
    def upload_to_s3(user_data_batch, batch_number):
        file_path = f'/opt/airflow/files/challenger_user_info_batch_{batch_number}.csv'
        user_data_batch.to_csv(file_path, index=False, header=False)
        hook = S3Hook('aws_default')
        hook.load_file(filename=file_path,
                       key=f'files/challenger_user_info_batch_{batch_number}.csv',
                       bucket_name='morzibucket',
                       replace=True)
        print(f"Uploaded batch {batch_number} to S3.")

    def save_batches_to_s3(**kwargs):
        user_data = kwargs['ti'].xcom_pull(task_ids='process_user_data')
        batch_size = 80
        
        for i in range(0, len(user_data), batch_size):
            user_batch = user_data.iloc[i:i + batch_size]  # 50명씩 배치
            upload_to_s3(user_batch, i // batch_size + 1)  # 배치 번호는 1부터 시작

    save_batches = PythonOperator(
        task_id='save_batches_to_s3',
        python_callable=save_batches_to_s3,
        provide_context=True
    )
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='fresh0911@naver.com',
        subject='S3 적재 성공',
        html_content='S3 적재 성공하였습니다.'
    )
    start >> process_user >> save_batches >> send_email_task