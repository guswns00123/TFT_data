from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum
import pandas as pd

def split_user_data(df, batch_size):
    total_users = df.shape[0]  # 총 유저 수
    for i in range(0, total_users, batch_size):
        yield df.iloc[i:i + batch_size]

# 경기 정보를 불러오는 함수 (각 배치당 50명의 유저)
def fetch_match_info_for_batch(user_batch):
    user_puuids = user_batch['puuid'].tolist()
    # 여기서 각 유저의 puuid를 사용하여 API 호출 로직을 추가하세요.
    for puuid in user_puuids:
        print(f"Fetching matches for user: {puuid}")
        # 경기 정보 API 호출 예시
        # fetch_match_data(puuid)
    return "Success"

# CSV 파일을 읽고 유저 데이터를 필터링하는 함수
def read_and_filter_csv():
    df = pd.read_csv(file_path)
    # CHALLENGER 유저만 필터링
    df_filtered = df[df['tier'] == 'CHALLENGER']
    return df_filtered

def load_data(**kwargs):
    df_filtered = read_and_filter_csv()
    # 필터링된 데이터 XCom으로 전달
    kwargs['ti'].xcom_push(key='filtered_data', value=df_filtered)
with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 10, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    load_data_task = PythonOperator(
    task_id='load_user_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Step 2: 50명씩 유저 데이터를 나누어 각 배치에 대해 경기 정보를 가져오는 태스크 생성
    batch_size = 50

    def create_batch_tasks(**kwargs):
        # XCom에서 필터링된 유저 데이터 가져오기
        df_filtered = kwargs['ti'].xcom_pull(key='filtered_data')
        
        for i, user_batch in enumerate(split_user_data(df_filtered, batch_size)):
            task = PythonOperator(
                task_id=f'fetch_match_info_batch_{i+1}',  # 고유한 태스크 ID 부여
                python_callable=fetch_match_info_for_batch,
                op_args=[user_batch],  # 배치 데이터를 함수에 전달
                dag=dag,
            )
            # Step 1과 각 batch 태스크를 연결
            load_data_task >> task

    create_batch_tasks_task = PythonOperator(
        task_id='create_batch_tasks',
        python_callable=create_batch_tasks,
        provide_context=True,
        dag=dag,
    )

    # Step 1 -> Step 2
    load_data_task >> create_batch_tasks_task