import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from src.task.e_crawling_traffic_accident import (
    e_crawling_latest_traffic_accident,
    headers,
    this_year_A1_url,
    this_year_A2_url,
)
from src.task.l_dim_accident_type import l_dim_accident_type
from src.task.l_dim_lane_design import l_dim_lane_design
from src.task.l_dim_road_design import l_dim_road_design
from src.task.l_fact_accident_env import l_fact_accident_env
from src.task.l_fact_accident_human import l_fact_accident_human
from src.task.l_fact_accident_main import l_fact_accident_main
from src.task.t_dim_accident_type import t_dim_accident_type
from src.task.t_dim_lane_design import t_dim_lane_design
from src.task.t_dim_road_design import t_dim_road_design
from src.task.t_fact_accident_env import t_fact_accident_env
from src.task.t_fact_accident_human import t_fact_accident_human
from src.task.t_fact_accident_main import t_fact_accident_main

# Default arguments for the DAG
default_args = {
    "owner": "jessie",  # DAG 擁有者名稱
    "depends_on_past": False,  # 任務是否依賴前一次DAG執行結果（False=獨立執行）
    "retries": 2,  # dag run失敗時最多重試2次，總計允許執行3次
    "retry_delay": timedelta(minutes=10),  # 除非task自己有額外定義，否則task重試需間隔10分鐘
}


@dag(
    dag_id="d02_track_recent_traffic_accidents",
    default_args=default_args,
    description="A ETL process from requesting data.gov.tw for the traffic accidents in 2026 until loading to MySQL database",
    schedule="00 11 */2 * *",  # 每月1、3、5、7、10、12日的11點00分執行一次
    start_date=datetime(2026, 4, 4, 17, 00,
                        tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=['traffic', 'taskflow'],
)
def traffic_accidents_pipeline():

    database = os.getenv("MYSQL_DATABASE")

    @task(retries=3, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(minutes=30))
    def task_e_crawler(this_year_A1_url, this_year_A2_url, headers):
        pathlist = e_crawling_latest_traffic_accident(this_year_A1_url, this_year_A2_url, headers)
        return pathlist

    @task
    def task_t_and_l_dim_tables(pathlist, database):

        t_done_type = t_dim_accident_type(pathlist)
        l_dim_accident_type(t_done_type, database)

        t_done_road = t_dim_road_design(pathlist)
        l_dim_road_design(t_done_road, database)

        t_done_lane = t_dim_lane_design(pathlist)
        l_dim_lane_design(t_done_lane, database)

        return None

    @task
    def task_t_and_l_fact_main(pathlist, database):
        t_done_main = t_fact_accident_main(pathlist)
        l_fact_accident_main(t_done_main, database)
        return None

    @task
    def task_t_and_l_other_facts(pathlist, database):
        t_done_human = t_fact_accident_human(pathlist)
        l_fact_accident_human(t_done_human, database)
        t_done_env = t_fact_accident_env(pathlist)
        l_fact_accident_env(t_done_env, database)
        return None

    e_done = task_e_crawler(this_year_A1_url, this_year_A2_url, headers)
    dim_done = task_t_and_l_dim_tables(e_done, database)
    fact_main_done = task_t_and_l_fact_main(e_done, database)
    fact_others_done = task_t_and_l_other_facts(e_done, database)
    e_done >> dim_done >> fact_main_done >> fact_others_done


# Instantiate the DAG
traffic_accidents_pipeline()
