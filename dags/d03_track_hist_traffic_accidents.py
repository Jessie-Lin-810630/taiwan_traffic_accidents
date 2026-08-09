"""DAG d03：回補 2021 至 2025 年的歷年交通事故資料並載入 MySQL。

與 d02 的流程相同，差別在資料來源是歷年的資料集頁面（一律是 ZIP），且排程為
`None`，需要回補時手動觸發。五個 task 串成一條線：抓檔 → 三張維度表 →
事故主檔 → 切檔案批次 → 其餘兩張事實表，順序不能調換。

最後一段走 dynamic task mapping，一個 mapped instance 處理一個檔案批次
（幾個 CSV 由 `task_prep_file_batches` 的 `batch_size` 決定）——五年份的 65 個
檔案一次載入需要約 10.6 GB，需注意 VM 記憶體 < 16 GB 的話會有負擔，參考 ADR-0015。
"""

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from src.task.e_crawling_traffic_accident import (
    e_crawling_historical_traffic_accident,
    headers,
    historical_years_urls,
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
    dag_id="d03_track_hist_traffic_accidents",
    default_args=default_args,
    description="A ETL process from requesting data.gov.tw for the traffic accidents btw 2021-2025 until loading to MySQL database",
    schedule=None,
    start_date=datetime(2026, 4, 4, 17, 00, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=["traffic", "taskflow"],
)
def traffic_accidents_pipeline_hist():
    """串接歷年事故資料的抓取、轉換與載入五個 task。"""
    database = os.getenv("MYSQL_DATABASE")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=30),
    )
    def task_e_crawler(historical_years_urls, headers):
        """下載歷年事故資料的 ZIP 並解壓，回傳 CSV 路徑清單。

        Args:
            historical_years_urls (list[str]): 歷年資料的資料集頁面網址。
            headers (dict): 請求標頭。

        Returns:
            list[str]: CSV 檔案路徑，交由下游 task 讀取。

        Raises:
            requests.exceptions.RequestException: 抓取或下載失敗且重試耗盡。
            zipfile.BadZipFile: 下載回來的檔案不是有效的 ZIP。
        """
        pathlist = e_crawling_historical_traffic_accident(
            historical_years_urls, headers
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
    def task_prep_file_batches(pathlist, batch_size: int = 2) -> list[list[str]]:
        """把 CSV 路徑清單切成每 `batch_size` 個一組，供下游動態展開。

        Args:
            pathlist (list[str]): 事故 CSV 的路徑清單，來自抓取 task。
            batch_size (int): 一個檔案批次涵蓋幾個 CSV，預設值 2 個

        Returns:
            list[list[str]]: 每個元素是一個檔案批次的路徑清單，清單長度為 > 0 且 <= `batch_size`。

        Raises:
            ValueError: 路徑清單為空，代表上游沒有抓到任何檔案。
        Notes:
            `batch_size` 取捨參考 ADR-0015。
        """
        if not pathlist:
            raise ValueError("pathlist 為空，上游未產出任何 CSV 檔")

        batches = [
            pathlist[i : i + batch_size] for i in range(0, len(pathlist), batch_size)
        ]
        return batches

    # 使用 accident_load_pool 將併發壓成 1，需先到 Airflow UI 建立並把 slots 設為 1，
    @task(pool="accident_load_pool")
    def task_t_and_l_other_facts(pathlist, database):
        """清洗並載入一個檔案批次的當事人與環境兩張事實表。

        必須排在主檔之後，因為兩者都要以主檔的事故編號作為外鍵。以
        `expand()` 動態展開，一個實例處理一個檔案批次；掛在 "accident_load_pool" 上
        讓批次序列執行，否則同時展開的任務實例會一起吃掉 VM 的記憶體。

        Args:
            pathlist (list[str]): 一個檔案批次的 CSV 路徑清單。
            database (str): 目標資料庫名稱。

        Returns:
            None: 本 task 只有副作用。

        Raises:
            ValueError: 路徑清單為空，或有列在 `fact_accident_main` 找不到對應的事故。
            pymysql.MySQLError: 寫入失敗（含外鍵約束不成立）。
        Notes:
            冪等性保證說明請參考 ADR-0015 決策四。
        """
        t_done_human = t_fact_accident_human(pathlist)
        l_fact_accident_human(t_done_human, database)
        t_done_env = t_fact_accident_env(pathlist)
        l_fact_accident_env(t_done_env, database)
        return None

    e_done = task_e_crawler(historical_years_urls, headers)
    dim_done = task_t_and_l_dim_tables(e_done, database)
    fact_main_done = task_t_and_l_fact_main(e_done, database)
    batches = task_prep_file_batches(e_done, 2)
    # MappedOperator。一個 mapped instance 處理 一份檔案批次
    fact_others_done = task_t_and_l_other_facts.partial(database=database).expand(
        pathlist=batches
    )
    e_done >> dim_done >> fact_main_done >> batches >> fact_others_done


# Instantiate the DAG
traffic_accidents_pipeline_hist()
