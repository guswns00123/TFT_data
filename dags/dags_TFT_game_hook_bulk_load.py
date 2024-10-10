from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator

# 챌린저 유저 정보 데이터를 local Postgres DB에 적재하는 DAG
with DAG(
        dag_id='dags_TFT_game_hook_bulk_load',
        start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    
    start = EmptyOperator(
    task_id='start'
    )

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    #data_version,match_id,participants
    insrt_postgres1 = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'tft_game_info',
                   'file_nm':'/opt/airflow/files/game_res/challenger_game_res_1.csv'}
    )

    
    insrt_postgres2 = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'tft_game_res',
                   'file_nm':'/opt/airflow/files/game_id/challenger_game_id_1.csv'}
    )
    start >> insrt_postgres1 >> insrt_postgres2