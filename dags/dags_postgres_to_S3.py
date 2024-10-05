from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator

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

    def process_user_data(postgres_conn_id, query, file_path, **kwargs):

        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        
        # 쿼리 실행 후 결과 가져오기
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        
        # 데이터를 CSV로 저장
        with open(file_path, "w") as f:
            for row in result:
                f.write(",".join([str(item) for item in row]) + "\n")
        
        print(f"Data saved to: {file_path}")
    process_user = PythonOperator(
        task_id="process_user_data",
        python_callable=process_user_data,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'query': 'SELECT * FROM tft_user_info',  # 실행할 쿼리
            'file_path': '/opt/airflow/files/tft_user_info2.csv'  # 파일 저장 경로
        }
    )
 
    def upload_to_s3(filename, key, bucket_name):
        hook = S3Hook('aws_default')
        hook.load_file(filename=filename,
                       key = key,
                       bucket_name=bucket_name,
                       replace=True)
    
    upload_s3 = PythonOperator(
        task_id = 'upload_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename' : '/opt/airflow/files/tft_user_info2.csv',
            'key' : 'files/tft_user_info2.csv',
            'bucket_name' : 'morzibucket'
        })
    
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='fresh0911@naver.com',
        subject='S3 적재 성공',
        html_content='S3 적재 성공하였습니다.'
    )
    process_user >> upload_s3 >> send_email_task