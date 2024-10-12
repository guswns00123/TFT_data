from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pendulum

with DAG(
    dag_id="dags_pyspark",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    # PySpark 작업 실행
    spark_submit = SparkSubmitOperator(
        task_id='submit_pyspark_job',
        application='/test.py',  # PySpark 스크립트 경로
        name='pyspark_job',
        conn_id='spark_default',  # Spark 연결 ID (Airflow에 Spark 연결을 설정해야 함)
        jars='/home/hdoop/postgresql-42.6.2.jar',  # 필요한 JAR 파일
        application_args=['arg1', 'arg2'],  # 필요 시 스크립트에 전달할 인자
    )

    spark_submit