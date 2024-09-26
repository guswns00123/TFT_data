from operators.TFT_api_to_csv_operator import TFTApiToCsvOperator
from airflow import DAG
import pendulum
from airflow.models import Variable

with DAG(
    dag_id='dags_tft_api',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''천상계 선수 데이터'''
    var_value = Variable.get("apikey_tft")
    test = TFTApiToCsvOperator(
        task_id='test',
        key=var_value,
        path='/opt/airflow/files/test/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='sky_user_list.csv'
    )
    

    test 