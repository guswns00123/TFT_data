  <h3 align="center">TFT Challenger User Weekly Dashboard</h3>

  <!-- ABOUT THE PROJECT -->
## About The Project

TFT(Teamfight Tactics) API를 활용해 최상위 챌린저 유저들의 플레이 패턴을 분석하고, 시간별로 게임 활동량을 파악할 수 있는 인사이트를 제공하는 프로젝트입니다.

챌린저 유저들이 특정 시간대에 게임을 가장 활발히 진행하는지 확인하고, 경기 데이터를 통해 플레이 스타일 및 성과를 분석하기 위해 시작되었습니다.

프로젝트 주요 목표

- 주기적 데이터 수집: Airflow를 이용해 API에서 자동으로 유저 및 경기 데이터를 수집.

- 효율적 데이터 저장 및 처리: AWS S3와 PostgreSQL DB를 활용해 대규모 데이터를 저장하고, Spark SQL로 분석.

- 시각화 및 인사이트 제공: Tableau를 사용해 유저 활동 패턴과 경기 성과를 시각화하여 이해하기 쉽게 제공.

### Built With
 <img src="https://img.shields.io/badge/Apache Ariflow-017CEE?style=flat&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Postgresql-4169E1?style=flat&logo=postgresql&logoColor=white"/>
    <img src="https://img.shields.io/badge/AWS S3-569A31?style=flat&logo=amazons3&logoColor=white"/>
    <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white"/>
    
## Main Features
![TFT유저 drawio (1)](https://github.com/user-attachments/assets/4e209e28-df59-4b76-930c-1904cd9fd5c7)

## Installation
```bash
git clone https://github.com/guswns00123/TFT_data.git

cd TFT_data # 보안상 docker-compose 파일은 git에는 생략

docker-compose up
```

## TFT 분석 데이터 ER 구조
![image (3)](https://github.com/user-attachments/assets/24a8160c-7604-49f8-abfa-37f95d9c94d9)

## 파이프 라인 설명

1) TFT 유저 데이터 적재 과정

    DAG ID : dags_postgres_to_S3_LocalDB
   
    주기: 매일 새벽 1시
    
    설명:
    
    Airflow DAG에서 API를 호출해 유저 데이터를 수집.
    
    PostgreSQL DB에 적재하여 저장.
    
    AWS S3에는 배치 처리 방식으로 여러 파일로 나누어 저장.
    
![image](https://github.com/user-attachments/assets/f2790b7a-4484-42b1-8060-a1d0f008def5)


2) TFT 경기 결과 데이터 적재 과정
   
    DAG ID: dags_game_info_to_LocalDB
    
    주기: 매 2시간 마다
  
    설명:
  
    Airflow DAG에서 Lambda를 병렬로 호출하여 유저별 경기 결과를 수집.
    
    수집된 데이터는 PostgreSQL DB,S3로 저장.
  
![image (1)](https://github.com/user-attachments/assets/09223e4b-91b6-4901-bd05-b59821056e2c)

## 주간 챌린저 유저 경기 결과 대쉬 보드

![image (2)](https://github.com/user-attachments/assets/75c7448c-eb60-4ee0-9351-dd1fc6eb167d)


