"""DAG d02：抓取今年度的 A1、A2 交通事故資料並載入 MySQL。

每月 1、10、20 日的 17 點跑一次。四個 task 串成一條線：抓檔 → 三張維度表 → 事故主檔 →
其餘兩張事實表。順序不能調換，因為事實表需要維度表的外鍵，而環境與當事人
兩張表又需要主檔的事故編號。

歷年（2021 至 2025 年）的同一套流程在 d03。
"""

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
    "retry_delay": timedelta(
        minutes=10
    ),  # 除非task自己有額外定義，否則task重試需間隔10分鐘
}


@dag(
    dag_id="d02_track_recent_traffic_accidents",
    default_args=default_args,
    description="A ETL process from requesting data.gov.tw for the traffic accidents in 2026 until loading to MySQL database",
    schedule="00 17 1,10,20 * *",  # 每月1、10、20日的17點00分執行一次
    start_date=datetime(2026, 4, 4, 17, 00, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=["traffic", "taskflow"],
)
def traffic_accidents_pipeline():
    """串接今年度事故資料的抓取、轉換與載入四個 task。"""
    database = os.getenv("MYSQL_DATABASE")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=30),
    )
    def task_e_crawler(this_year_A1_url, this_year_A2_url, headers):
        """下載今年的 A1、A2 事故資料，回傳解出來的 CSV 路徑清單。

        Args:
            this_year_A1_url (list[str]): 今年 A1 資料的資料集頁面網址。
            this_year_A2_url (list[str]): 今年 A2 資料的資料集頁面網址。
            headers (dict): 請求標頭。

        Returns:
            list[str]: CSV 檔案路徑，交由下游 task 讀取。

        Raises:
            requests.exceptions.RequestException: 抓取或下載失敗且重試耗盡。
        """
        pathlist = e_crawling_latest_traffic_accident(
            this_year_A1_url, this_year_A2_url, headers
        )
        return pathlist

    @task
    def task_t_and_l_dim_tables(pathlist, database):
        """依序清洗並載入事故類別、道路設計與車道設計三張維度表。

        三張表彼此無相依，合併在同一個 task 內是因為它們讀的是同一批 CSV。

        Args:
            pathlist (list[str]): 事故 CSV 的路徑清單，來自抓取 task。
            database (str): 目標資料庫名稱。

        Returns:
            None: 本 task 只有副作用。

        Raises:
            ValueError: 路徑清單為空，代表上游沒有抓到任何檔案。
            pymysql.MySQLError: 寫入失敗。
        """
        t_done_type = t_dim_accident_type(pathlist)
        l_dim_accident_type(t_done_type, database)

        t_done_road = t_dim_road_design(pathlist)
        l_dim_road_design(t_done_road, database)

        t_done_lane = t_dim_lane_design(pathlist)
        l_dim_lane_design(t_done_lane, database)

        return None

    @task
    def task_t_and_l_fact_main(pathlist, database):
        """清洗並載入事故主檔 `fact_accident_main`。

        必須排在維度表之後，因為主檔要查維度表取得外鍵。

        Args:
            pathlist (list[str]): 事故 CSV 的路徑清單，來自抓取 task。
            database (str): 目標資料庫名稱。

        Returns:
            None: 本 task 只有副作用。

        Raises:
            ValueError: 路徑清單為空。
            pymysql.MySQLError: 寫入失敗（含外鍵約束不成立）。
        """
        t_done_main = t_fact_accident_main(pathlist)
        l_fact_accident_main(t_done_main, database)
        return None

    @task
    def task_t_and_l_other_facts(pathlist, database):
        """清洗並載入當事人與環境兩張事實表。

        必須排在主檔之後，因為兩者都要以主檔的事故編號作為外鍵。

        Args:
            pathlist (list[str]): 事故 CSV 的路徑清單，來自抓取 task。
            database (str): 目標資料庫名稱。

        Returns:
            None: 本 task 只有副作用。

        Raises:
            ValueError: 路徑清單為空。
            pymysql.MySQLError: 寫入失敗（含外鍵約束不成立）。
        """
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
