from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
import os
import pandas as pd
from hooks.custom_postgres_hook import CustomPostgresHook

def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)
def process_user_data(postgres_conn_id, query, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        return pd.read_sql_query(query, con=postgres_hook.get_conn(), chunksize=1000)

def upload_to_s3(user_data_batch, batch_number):
        file_path = f'/opt/airflow/files/challenger_user_info_batch_{batch_number}.csv'
        user_data_batch.to_csv(file_path, index=False, header=False)
        hook = S3Hook('aws_default')
        hook.load_file(filename=file_path,
                       key=f'files/challenger_user_info_batch_{batch_number}.csv',
                       bucket_name='morzibucket',
                       replace=True)
        os.remove(file_path)  # 업로드 후 파일 삭제
        print(f"Uploaded batch {batch_number} to S3.")

def save_batches_to_s3(**kwargs):
        postgres_conn_id = kwargs['postgres_conn_id']
        query = 'SELECT * FROM user_info'
        
        for batch_number, user_data_batch in enumerate(process_user_data(postgres_conn_id, query)):
            upload_to_s3(user_data_batch, batch_number + 1)
with DAG(
        dag_id='test2',
        start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
        schedule='0 1 * * *',  # 매일 새벽 1시
        catchup=False
) as dag:
    start = EmptyOperator(task_id='start')
    
    insrt_postgres1 = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'tbl_nm': 'user_info',
            'file_nm': '/opt/airflow/files/challenger_user_data.csv'
        }
    )

    save_batches = PythonOperator(
        task_id='save_batches_to_s3',
        python_callable=save_batches_to_s3,
        provide_context=True,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom'
        }
    )
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='fresh0911@naver.com',
        subject='S3 적재 성공',
        html_content='S3 적재 성공하였습니다.'
    )

    start >> insrt_postgres1 >> save_batches >> send_email_task
