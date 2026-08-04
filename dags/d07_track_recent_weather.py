"""d07：抓取本年度至今的天氣觀測，並載入 MySQL。"""

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import TaskGroup, dag, task

from src.task.e_crawling_weather import (
    e_crawler_weatherapi,
    e_get_uniq_acc_geo,
    prep_batch_plan,
)
from src.task.l_fact_hourly_weather import l_fact_hourly_weather

# Default arguments for the DAG
default_args = {
    "owner": "jessie",  # DAG 擁有者名稱
    "depends_on_past": False,  # 任務是否依賴前一次執行結果（False=獨立執行）
    "retries": 2,  # dag run失敗時最多重試2次，總計允許執行3次
    "retry_delay": timedelta(
        minutes=10
    ),  # 除非task自己有額外定義，否則task重試需間隔10分鐘
}


@dag(
    dag_id="d07_track_recent_weather",
    default_args=default_args,
    description="ETL process from requesting weather API for the weather data between 'January 01st~3-day-prior-to-today' until loading to MySQL database",
    schedule="00 07 */3 * *",  # 每月1、4、7、10、13、16日的07點00分執行一次
    start_date=datetime(2026, 2, 25, 5, 00, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=["traffic", "weatherapi", "taskflow"],
)
def accident_weather_pipeline():
    """串接本年度天氣 ETL：取觀測點 -> 制訂批次 -> 抓取 -> 清洗載入。"""
    this_year = 2026
    database = os.getenv("MYSQL_DATABASE")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=30),
    )
    def task_e_get_uniq_acc_geo(target_year: int, database: str | None):
        return e_get_uniq_acc_geo(target_year, database=database)

    @task
    def task_prep_batch_plan(
        df_acc_unique_loc, target_year: int, batch_size: int
    ) -> list[int]:
        return prep_batch_plan(df_acc_unique_loc, target_year, batch_size)

    @task(
        pool="weather_api_pool",  # 需到UI進一步給值，指一次可以執行多少個同類task
        retries=3,  # 如果出現except，最多再重試3次，總計每個task可跑4次
        retry_delay=timedelta(minutes=20),  # 20分鐘後才重試
        retry_exponential_backoff=True,  # 讓等待時間隨次數增加(指數退避)
        max_retry_delay=timedelta(hours=2),  # 指數退避下，最長間隔2小時後重試
        # 排除等待時間，如果執行總時間超過2小時，殺掉該task避免佔用pool資源
        execution_timeout=timedelta(hours=2),
        do_xcom_push=False,  # 回傳的xcom不推送到下一個task，省掉存xcom的記憶體空間
    )
    def task_e_crawler_weatherapi(batch_id: int, target_year: int) -> str:
        return e_crawler_weatherapi(batch_id, target_year)

    @task(
        retries=2,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=4),
    )
    def task_t_and_l_weather(target_year: int, database: str | None) -> None:
        l_fact_hourly_weather(target_year, database=database, batch_size=50)

    with TaskGroup(group_id=f"year_{this_year}"):
        df = task_e_get_uniq_acc_geo(this_year, database)
        batches = task_prep_batch_plan(df, this_year, 50)
        # MappedOperator
        craw_done = task_e_crawler_weatherapi.partial(target_year=this_year).expand(
            batch_id=batches
        )

        load_done = task_t_and_l_weather(this_year, database)
        craw_done >> load_done


# Instantiate the DAG
accident_weather_pipeline()
