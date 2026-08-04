"""d08：逐年回補歷史年度的天氣觀測，並載入 MySQL。"""

import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.sdk import TaskGroup, dag, task

# 1. 先確保opt/airflow有在sys.path中，以確保python interpreter能找到./tasks ./utils下的模組或套件
if "/opt/airflow" not in sys.path:
    sys.path.append("/opt/airflow")

# 2. 在sys.path之後才進行import
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
    schedule=None,
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
        retries=2,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=4),
    )
    def task_t_and_l_weather(target_year: int, database: str | None) -> None:
        l_fact_hourly_weather(target_year, database=database, batch_size=50)

    for year in target_years:
        with TaskGroup(group_id=f"year_{year}") as year_group:
            df = e_get_uniq_acc_geo(year, database=database)
            batches = prep_batch_plan(df, year, batch_size=50)
            # MappedOperator
            craw_done = e_crawler_weatherapi.partial(target_year=year).expand(
                batch_id=batches
            )

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
