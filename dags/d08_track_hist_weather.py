"""d08：逐年回補歷史年度的天氣觀測，並載入 MySQL。"""

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
    dag_id="d08_track_hist_weather",
    default_args=default_args,
    description="ETL process from requesting weather API for the weather data between 'January 01st~December 31th' until loading to MySQL database",
    # 四年回補約 61,320 次額度，受日限額 10,000 約束，需連續跑約 7 天。
    # 每天 09:00 讓 d07（07:00）先取走當年度要的額度，剩下的才給回補。
    # 補完之後手動 pause 掉這支 DAG —— 它是一次性工作（ADR-0013 決策七）。
    schedule="00 09 * * *",
    start_date=datetime(2026, 2, 25, 5, 00, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=["traffic", "weatherapi", "taskflow"],
)
def accident_weather_pipeline():
    """逐年串接天氣 ETL，前一年載入完成後才開始下一年。"""
    target_years = [2021, 2022, 2023, 2024]
    database = os.getenv("MYSQL_DATABASE")
    previous_year_group = None

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
        # 回補必然跨多天才抓得完，已落地 GCS 的資料要先進 MySQL（ADR-0013 決策六）
        trigger_rule="all_done",
    )
    def task_t_and_l_weather(target_year: int, database: str | None) -> None:
        l_fact_hourly_weather(target_year, database=database, batch_size=50)

    for year in target_years:
        with TaskGroup(group_id=f"year_{year}") as year_group:
            df = task_e_get_uniq_acc_geo.override(
                task_id=f"task_e_get_uniq_acc_geo_{year}"
            )(year, database)
            batches = task_prep_batch_plan.override(
                task_id=f"task_prep_batch_plan_{year}"
            )(df, year, 50)
            # MappedOperator。一個 mapped instance = 一批觀測點 × 一個月，
            # 參數由 prep_batch_plan() 以 dict 排好（ADR-0013 決策四）。
            craw_done = task_e_crawler_weatherapi.override(
                task_id=f"task_e_crawler_weatherapi_{year}"
            ).expand_kwargs(batches)

            load_done = task_t_and_l_weather.override(
                task_id=f"task_t_and_l_weather_{year}"
            )(year, database)
            craw_done >> load_done

        # 一年loading完之後換成下一年e_get_unqi_acc_geo執行
        if previous_year_group:
            previous_year_group >> year_group

        previous_year_group = year_group


# Instantiate the DAG
accident_weather_pipeline()
