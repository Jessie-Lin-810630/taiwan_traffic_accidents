"""DAG d05：爬取夜市清單與 Google Maps 地理資訊，載入 `fact_night_markets`。"""

import os
from datetime import timedelta

from airflow.sdk import dag, task

from src.task.create_night_markets_tables import create_night_market_tables
from src.task.e_crawling_nightmarket import (
    cities_per_region,
    e_crawling_nightmarket,
    find_tw_night_markets_list,
    headers,
    night_markets_wiki_url,
)
from src.task.t_fact_night_markets import (
    read_googlemap_responsed_json,
    t_fact_night_markets,
)
from src.util.mysql_utils import get_engine_to_mysql

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
    dag_id="d05_track_night_markets",
    default_args=default_args,
    description="A ETL process from requesting GoogleMaps API for the latest night markets until loading to MySQL database",
    schedule="00 11 05 * *",  # 每月5日的11點00分執行一次
    start_date=None,
    catchup=False,
    tags=["night_markets", "GoogleMap", "taskflow"],
)
def night_markets_pipeline():
    """建表後爬取夜市資料，轉換並載入夜市事實表。"""
    database = os.getenv("MYSQL_DATABASE")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=10),
    )
    def task_crx_nm_table(database):
        engine = get_engine_to_mysql(database)
        create_night_market_tables(engine)
        return None

    @task
    def task_e_night_markets(night_markets_wiki_url, headers, cities_per_region):
        file_path_to_nm_lst = find_tw_night_markets_list(
            night_markets_wiki_url, headers, cities_per_region
        )
        responsed_file_path = e_crawling_nightmarket(file_path_to_nm_lst)
        return responsed_file_path

    @task
    def task_t_and_l_night_markets(
        responsed_file_path: str, cities_per_region, database
    ):
        nm_info_lst = read_googlemap_responsed_json(responsed_file_path)
        t_fact_night_markets(nm_info_lst, cities_per_region, database, 10)
        return None

    crx_done = task_crx_nm_table(database)
    e_done = task_e_night_markets(night_markets_wiki_url, headers, cities_per_region)
    t_l_done = task_t_and_l_night_markets(e_done, cities_per_region, database)
    crx_done >> e_done >> t_l_done


night_markets_pipeline()
