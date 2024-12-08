from operators.TFT_api_to_csv_operator import TFTApiToCsvOperator
from operators.sky_get_id_operator import TFTApiToCsvOperator2
from airflow import DAG
import pendulum
from airflow.models import Variable
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_tft_api',
    schedule = '0 1 * * 1', #매주 월요일 새벽 1시sdsdsd
    start_date=pendulum.datetime(2024,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    '''천상계 선수 데이터'''
    var_value = Variable.get("apikey_tft")
    get_high_user_list = TFTApiToCsvOperator(
        task_id='get_high_user_list',
        a = var_value,
        path='/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='sky_user_list.csv'
    )

    get_high_user_puuid = TFTApiToCsvOperator2(
        task_id='get_high_user_puuid',
        a = var_value,
        path='/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='sky_user_list.csv'
    )
    

    get_high_user_list >> get_high_user_puuid 