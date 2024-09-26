from airflow.models.baseoperator import BaseOperator
import pandas as pd 
import time
import requests
from airflow.models import Variable

class TFTApiToCsvOperator(BaseOperator):
    template_fields = ('path','file_name','base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.file_name = file_name
        self.var_value = Variable.get("apikey_tft")
        self.base_dt = base_dt

    def execute(self, context):
        import os

        self.base_url = f'https://kr.api.riotgames.com/tft/'

        tier_list = ["challenger"]
        user_data = None
        for i in tier_list:
            users = self.extract_sky(i,self.base_url)
            if user_data is None:
                user_data = pd.DataFrame(users)
            else:
                user_data2 = pd.DataFrame(users)
                user_data = pd.concat([user_data, user_data2])
        #2
        #id를 이용하여 puuid 가져와 high_df에 적재
        high_df = None
        for index, row in user_data.iterrows():
            id = row['entries']['summonerId']
            if high_df is None:
                high_df = pd.DataFrame(self.extract_game_by_summoner(id, self.base_url))
            else:
                high_df2 = pd.DataFrame(self.extract_game_by_summoner(id, self.base_url))
                high_df = pd.concat([high_df, high_df2])
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        high_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def extract_sky(self, tier, base_url):

        if tier not in ["challenger", "grandmaster"]:

            print('해당 함수에서는 master, grandmaster, challenger만 기술할 수 있습니다.')

            return None
        request_header  = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://developer.riotgames.com",
            "X-Riot-Token": self.var_value
        }
        code_name = f"league/v1/{tier}"
        account_id = requests.get(f"{base_url}{code_name}", headers=request_header).json()
        
        return account_id
    
    def extract_game_by_summoner(self, idname, base_url):
        request_header  = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://developer.riotgames.com",
            "X-Riot-Token": self.var_value
        }
        id_code_name = base_url + "league/v1/entries/by-summoner/" + idname
        user_id = requests.get(id_code_name, headers = request_header).json()
        time.sleep(2)

        return user_id