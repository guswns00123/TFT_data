from operators.TFT_api_to_csv_operator import TFTApiToCsvOperator
from airflow import DAG
import pendulum
from airflow.models import Variable
from airflow.operators.python import PythonOperator

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
        a = var_value,
        path='/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='sky_user_list.csv'
    )

    def get_puuid_from_id(id,**kwargs):
        import requests
        import time
        base_url = f'https://kr.api.riotgames.com/tft/'
        request_header  = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://developer.riotgames.com",
            "X-Riot-Token": var_value
        }
        
        id_code_name = base_url + "league/v1/entries/by-summoner/" + id
        user_id = requests.get(id_code_name, headers = request_header).json()
        time.sleep(2)
        return user_id
    
    def load_puuid(**kwargs):
        import pandas as pd
        import os
        high_df = None
        path='/opt/airflow/files/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='sky_user_list.csv'
        final_file = 'sky_puuid.csv'
        user_data = pd.read_csv( path+'/' + file_name)

        for index, row in user_data.iterrows():
            id = row['entries']['summonerId']
            if high_df is None:
                high_df = pd.DataFrame(get_puuid_from_id(id))
            else:
                high_df2 = pd.DataFrame(get_puuid_from_id(id))
                high_df = pd.concat([high_df, high_df2])

        if not os.path.exists(path):
            os.system(f'mkdir -p {path}')

        high_df.to_csv(path + '/' + final_file, encoding='utf-8', index=False)

    test2 = PythonOperator(
        task_id='task_b',
        python_callable= load_puuid,
        op_kwargs={'selected':'B'}
    )
    

    test >> test2