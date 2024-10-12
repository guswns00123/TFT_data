from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='dags_pyspark',
    default_args=default_args,
    schedule_interval=None,  # 실행 주기 설정
    catchup=False,
) as dag:

    # PySpark 작업 실행
    spark_submit = SparkSubmitOperator(
        task_id='submit_pyspark_job',
        application='/home/hdoop/test.py',  # PySpark 스크립트 경로
        name='pyspark_job',
        conn_id='spark_default',  # Spark 연결 ID (Airflow에 Spark 연결을 설정해야 함)
        jars='/home/hdoop/postgresql-42.6.2.jar',  # 필요한 JAR 파일
        application_args=['arg1', 'arg2'],  # 필요 시 스크립트에 전달할 인자
    )

    spark_submit