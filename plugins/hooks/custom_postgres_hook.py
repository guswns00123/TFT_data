from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd
import json
import re
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
        from sqlalchemy import create_engine, event
        from sqlalchemy.engine import Engine
        import re

        
        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn()
        header = 0 if is_header else None                       # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append'       # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=0, delimiter=delimiter, index_col = None)
        if table_name == 'user_info':
            file_df.rename(columns={'puuid':'user_id'}, inplace=True)
            del file_df['ratedTier']
            del file_df['ratedRating']
            del file_df['leagueId']
            
            


        if table_name == 'game_info':
            new_tb_name = 'user_game'
            new_df = pd.DataFrame()
            self.log.info(file_df['match_id'][3:])
            new_df['user_game_id'] = file_df['participants'].str[:7] + '_' + file_df['match_id'].str[3:]
            new_df['user_id'] = file_df['participants']
            new_df['match_id'] = file_df['match_id'] 
            uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
            engine = create_engine(uri)
            new_df.to_sql(name=new_tb_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                        )
            
            del file_df['participants']
            file_df = file_df.drop_duplicates(subset=['match_id'])
            if_exists = 'append'
        if table_name =='game_result' :
            
            def fix_json_format(participant_str):
                participant_str = participant_str.replace("'", '"')
                participant_str = re.sub(r'\bFalse\b', 'false', participant_str)  # False -> false
                participant_str = re.sub(r'\bTrue\b', 'true', participant_str)  
                return participant_str

            file_df['participants'] = file_df['participants'].apply(fix_json_format)   

            def parse_json(participant_str):
                return json.loads(participant_str)

            file_df['participants'] = file_df['participants'].apply(parse_json)
  
            def flatten_participant(participant):
                flat_dict = {}
                self.log.info(participant['augments'])
                flat_dict['augments'] = ', '.join(participant['augments'])  # Join augments into a single string
                flat_dict['gold_left'] = participant['gold_left']
                flat_dict['last_round'] = participant['last_round']
                flat_dict['level'] = participant['level']
                flat_dict['placement'] = participant['placement']
                flat_dict['players_eliminated'] = participant['players_eliminated']
                flat_dict['puuid'] = participant['puuid']
                flat_dict['total_damage_to_players'] = participant['total_damage_to_players']
                companion = participant['companion']
                flat_dict['companion_species'] = companion['species']
                flat_dict['companion_item_ID'] = companion['item_ID']
                flat_dict['traits'] = ', '.join([trait['name'] for trait in participant['traits']])
                flat_dict['units'] = ', '.join([unit['character_id'] for unit in participant['units']])
                
                return flat_dict
        
            # Apply the flattening function to the 'participants' column
            flattened_data = file_df['participants'].apply(flatten_participant)
            
            # Convert the result to a DataFrame and concatenate with the original, dropping 'participants' column
            flattened_df = pd.DataFrame(flattened_data.tolist())
            file_df = pd.concat([file_df, flattened_df], axis=1).drop(columns=['participants'])
            file_df.rename(columns={'puuid':'user_id'}, inplace=True)
            del file_df['queueId']
            del file_df['queue_id']
            del file_df['mapId']
            del file_df['tft_game_type']
            del file_df['tft_set_core_name']
            del file_df['tft_set_number']
            tb1 = 'match_trait'
            tb2 = 'match_unit'
            tb3 = 'match_augment'
            df1 = pd.DataFrame()
            df2 = pd.DataFrame()
            df3 = pd.DataFrame()
            for index, row in file_df.iterrows():
                # traits 처리
                traits_list = row['traits'].split(', ')
                df1_new = pd.DataFrame({
                    'user_game_id': row['user_id'][:7] + '_' + str(row['gameId']),
                    'trait_id': traits_list
                })
                df1 = pd.concat([df1, df1_new], ignore_index=True)

                # units 처리
                unit_list = row['units'].split(', ')
                df2_new = pd.DataFrame({
                    'user_game_id': row['user_id'][:7] + '_' + str(row['gameId']),
                    'unit_id': unit_list
                })
                df2 = pd.concat([df2, df2_new], ignore_index=True)

                # augments 처리
                augment_list = row['augments'].split(', ')
                df3_new = pd.DataFrame({
                    'user_game_id': row['user_id'][:7] + '_' + str(row['gameId']),
                    'augment_id': augment_list
                })
                df3 = pd.concat([df3, df3_new], ignore_index=True)

            del file_df['traits']
            del file_df['units']
            del file_df['augments']
            
            file_df['user_game_id'] = file_df['user_id'].str[:7] + '_' + file_df['gameId'].astype(str)
            uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
            engine = create_engine(uri)
            df1.to_sql(name=tb1,
                            con=engine,
                            schema='public',
                            if_exists='append',
                            index=False
                        )
            df2.to_sql(name=tb2,
                            con=engine,
                            schema='public',
                            if_exists='append',
                            index=False
                        )
            df3.to_sql(name=tb3,
                            con=engine,
                            schema='public',
                            if_exists='append',
                            index=False
                        )
            if_exists = 'append'

        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        # file_df.columns = file_df.columns.str.lower()
        file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists="append",
                            index=False
                        )