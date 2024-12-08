from airflow.models.baseoperator import BaseOperator
import pandas as pd 
import time
import requests
from airflow.models import Variable

class TFTApiToCsvOperator(BaseOperator):
    template_fields = ('a','path','file_name','base_dt')

    def __init__(self, a, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.a = a
        self.path = path
        self.file_name = file_name
        self.base_dt = base_dt

    def execute(self, context):
        import os
        def extract_summoner_id(entries_dict):
            try:
                # entries_dict가 이미 딕셔너리 형태라고 가정
                return entries_dict['summonerId']
            except KeyError as e:
                print(f"KeyError: {e} - Entry: {entries_dict}")
                return None  # 오류가 발생하면 None 반환

        # entries 칼럼이 이미 딕셔너리 형태라면
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

        user_data['summonerId'] = user_data['entries'].apply(extract_summoner_id)
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        user_data.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)


        
        

    def extract_sky(self, tier, base_url):

        if tier not in ["challenger"]:
            print('해당 함수에서는 master, grandmaster, challenger만 기술할 수 있습니다.')
            return None
        request_header  = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://developer.riotgames.com",
            "X-Riot-Token": self.a
        }
        code_name = f"league/v1/{tier}"
        account_id = requests.get(f"{base_url}{code_name}", headers=request_header).json()
        
        return account_id
    
    