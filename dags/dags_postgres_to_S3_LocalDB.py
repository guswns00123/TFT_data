from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from hooks.custom_postgres_hook import CustomPostgresHook


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
    batch_size = 1000
    for i in range(0, len(user_data), batch_size):
        user_batch = user_data.iloc[i:i + batch_size]  
        upload_to_s3(user_batch, i // batch_size + 1)  

def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
    custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
    custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

def process_user_data(postgres_conn_id, query, **kwargs):
    import pandas as pd
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    return df

with DAG(
        dag_id='dags_postgres_to_S3_LocalDB',
        start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
        schedule='0 2 * * *', # 매일 새벽 1시
        catchup=False
) as dag:
    start = EmptyOperator(
    task_id='start'
    )
    
    insrt_postgres1 = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'user_info',
                   'file_nm':'/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/sky_puuid.csv'}
    )

    process_user = PythonOperator(
        task_id="process_user_data",
        python_callable=process_user_data,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'query': 'SELECT user_id FROM user_info', 
        },
        provide_context=True
    )
    
    
    save_batches = PythonOperator(
        task_id='save_batches_to_s3',
        python_callable=save_batches_to_s3,
        provide_context=True
    )
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='fresh0911@naver.com',
        subject='유저 정보 S3 적재 성공',
        html_content='S3 적재 성공하였습니다.'
    )

    start >> insrt_postgres1 >> process_user >>save_batches >> send_email_task