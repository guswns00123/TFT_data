from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from hooks.custom_postgres_hook import CustomPostgresHook

with DAG(
        dag_id='dags_postgres_operator',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    
    # def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
    #     custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
    #     custom_postgres_hook.

    # insrt_postgres = PythonOperator(
    #     task_id='insrt_postgres',
    #     python_callable=insrt_postgres,
    #     op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
    #                'tbl_nm':'TFT_user_info',
    #                'file_nm':'/opt/airflow/files/user_info.csv'}
    # )

    def process_user_data(ti):
    # 이전 task의 XCom에서 데이터를 가져옴
        query_result = ti.xcom_pull(task_ids="get_user")
    
    # query_result에는 쿼리 결과의 리스트가 들어있음
        if query_result:
            for row in query_result:
                print(f"User Info: {row}")

    load_user = SQLExecuteQueryOperator(
        task_id="get_user",
        conn_id="conn-db-postgres-custom",
        sql="SELECT * FROM tft_user_info;",
)
    process_user = PythonOperator(
        task_id="process_user_data",
        python_callable=process_user_data
    )
    

    load_user >> process_user