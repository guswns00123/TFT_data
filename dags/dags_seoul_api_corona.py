from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 코로나19 확진자 발생동향'''
    test = SeoulApiToCsvOperator(
        task_id='test',
        dataset_nm='league/v1/',
        path='/opt/airflow/files/test/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='test.csv'
    )
    

    test 