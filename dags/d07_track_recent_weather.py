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
    # 每月 3 號與 18 號的 07:00 各執行一次。
    #
    # 3 號：上個月的最後一天剛進入 API 查得到的範圍（延遲 3 天），該月因此
    #       判定為抓完整，整月抓一次後永不重抓。額度約 1,277（時限額的 26%）。
    # 18 號：上個月已排除，只抓當月 1～15 日。額度約 630（13%）。
    #
    # 頻率的依據是**事故資料兩週發布一次** —— 天氣是 inner join 掛在事故上的，
    # 抓得比事故新沒有意義。原本每 3 天一次會把同一個月重抓 10 次，
    # 其中 9 次的資料都會被下一次覆蓋，一年白花約 6 萬次額度。
    schedule="00 07 3,18 * *",
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
    ) -> list[dict]:
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
    def task_e_crawler_weatherapi(batch_id: int, target_year: int, month: int) -> str:
        return e_crawler_weatherapi(batch_id, target_year, month)

    @task(
        retries=2,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=4),
        # 抓取被額度打斷是這個設計的正常狀態，不是故障 —— 首次回補必然跨多次
        # DAG run 才抓得完。已經落地 GCS 的資料要先進 MySQL，不必等全部抓完
        # （ADR-0013 決策六）。
        trigger_rule="all_done",
    )
    def task_t_and_l_weather(target_year: int, database: str | None) -> None:
        l_fact_hourly_weather(target_year, database=database, batch_size=50)

    with TaskGroup(group_id=f"year_{this_year}"):
        df = task_e_get_uniq_acc_geo(this_year, database)
        batches = task_prep_batch_plan(df, this_year, 50)
        # MappedOperator。一個 mapped instance = 一批觀測點 × 一個月，
        # 參數由 prep_batch_plan() 以 dict 排好（ADR-0013 決策四）。
        craw_done = task_e_crawler_weatherapi.expand_kwargs(batches)

        load_done = task_t_and_l_weather(this_year, database)
        craw_done >> load_done


# Instantiate the DAG
accident_weather_pipeline()
