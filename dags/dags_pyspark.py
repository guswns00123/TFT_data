from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_pyspark",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    # PySpark 작업 실행
    spark_submit_task = BashOperator(
    task_id='spark_submit_task',
    bash_command='spark-submit --jars /home/hdoop/postgresql-42.6.2.jar /home/hdoop/test.py',
    dag=dag,
)

# DAG에 작업 추가
    spark_submit_task