import logging

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(dag_id="sample_repeated_mapping",
         start_date=datetime(2022, 6, 4),
         catchup=False
        ) as dag:

    @task
    def add(x: int, y: int):
        return x + y

    @task
    def print_x(x: list):
        logging.info(x)

    added_values = add.expand(x=[2, 4, 8], y=[5, 10])
    print_x(added_values)