from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator

# 챌린저 유저 정보 데이터를 local Postgres DB에 적재하는 DAG
with DAG(
        dag_id='dags_TFT_hook_bulk_load',
        start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    '''챌린저 유저 정보 데이터를 local Postgres DB에 적재 DAG'''
    start = EmptyOperator(
    task_id='start'
    )

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    
    insrt_postgres1 = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'tft_user_info',
                   'file_nm':'/opt/airflow/files/challenger_user_data.csv'}
    )

    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='fresh0911@naver.com',
        subject='Postgres DB 적재 성공',
        html_content='Postgres DB 적재 성공하였습니다.'
    )
    trigger_S3_load_task = TriggerDagRunOperator(
        task_id='trigger_S3_load_task',
        trigger_dag_id='dags_postgres_to_S3',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
        )
    
    start >> insrt_postgres1 >> send_email_task >> trigger_S3_load_task