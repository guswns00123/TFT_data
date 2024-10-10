from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd
import json

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn()
        header = 0 if is_header else None                       # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append'       # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)
        if table_name =='tft_game_res' and 'participants' in file_df.columns:
        
        # Function to flatten participant dictionary
            def flatten_participant(participant):
                flat_dict = {}
                flat_dict['augments'] = ', '.join(participant.get('augments', []))  # augments를 하나의 문자열로 결합
                flat_dict['gold_left'] = participant.get('gold_left', 0)
                flat_dict['last_round'] = participant.get('last_round', 0)
                flat_dict['level'] = participant.get('level', 0)
                flat_dict['placement'] = participant.get('placement', 0)
                flat_dict['players_eliminated'] = participant.get('players_eliminated', 0)
                flat_dict['puuid'] = participant.get('puuid', '')
                flat_dict['total_damage_to_players'] = participant.get('total_damage_to_players', 0)
                
                # companion 데이터 추출
                companion = participant.get('companion', {})
                flat_dict['companion_species'] = companion.get('species', '')
                flat_dict['companion_item_ID'] = companion.get('item_ID', 0)
                
                # traits와 units 추출
                flat_dict['traits'] = ', '.join([trait['name'] for trait in participant.get('traits', [])])
                flat_dict['units'] = ', '.join([unit['character_id'] for unit in participant.get('units', [])])
                
                return flat_dict
            def parse_json(x):
                try:
                    return json.loads(x) if isinstance(x, str) else x
                except json.JSONDecodeError as e:
                    self.log.error(f"JSON 파싱 오류 발생: {x}, 오류: {e}")
                    return {}  # 오류 발생 시 빈 딕셔너리 반환

        # JSON 파싱 후 처리
            file_df['participants'] = file_df['participants'].apply(parse_json)
            # Apply the flattening function to the 'participants' column
            flattened_data = file_df['participants'].apply(flatten_participant)
            
            # Convert the result to a DataFrame and concatenate with the original, dropping 'participants' column
            flattened_df = pd.DataFrame(flattened_data.tolist())
            file_df = pd.concat([file_df, flattened_df], axis=1).drop(columns=['participants'])

        for col in file_df.columns:                             
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n','')      # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue 
                
        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                        )