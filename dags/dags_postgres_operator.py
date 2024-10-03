from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
        dag_id='dags_postgres_operator',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
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
        sql="SELECT * FROM TFT_user_info;",
)
    process_user = PythonOperator(
        task_id="process_user_data",
        python_callable=process_user_data
    )
    

    load_user >> process_user