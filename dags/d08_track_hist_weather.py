"""DAG d08：逐年回補歷史年度（2021 至 2025 年）的天氣觀測，並載入 MySQL。

一次性的回補工作。五年份量約需連續跑八-九天才抓得完，因此每天 09:00 跑一次，
排在 d07（07:00）之後，讓當年度先取走需要的 API 額度。五個年度依序執行，
前一年載入完成後才開始下一年。**補完之後請手動 pause 這支 DAG。**

task 的組成與 d07 相同，差別只在多包一層年度迴圈。
"""

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
    # 五年回補約 76,650 次額度，受日限額 10,000 約束，需連續跑約 8-9 天。
    # 每天 09:00 讓 d07（07:00）先取走當年度要的額度，剩下的才給回補。
    # 補完之後手動 pause 掉這支 DAG，它是一次性工作。
    schedule="00 09 * * *",
    start_date=datetime(2026, 2, 25, 5, 00, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=["traffic", "weatherapi", "taskflow"],
)
def accident_weather_pipeline():
    """逐年串接天氣 ETL，前一年載入完成後才開始下一年。"""
    target_years = [2021, 2022, 2023, 2024, 2025]
    database = os.getenv("MYSQL_DATABASE")
    previous_year_group = None

    @task(
        retries=3,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=30),
    )
    def task_e_get_uniq_acc_geo(target_year: int, database: str | None):
        """查出該年度事故發生地的座標，進位到氣象網格後去重。

        Args:
            target_year (int): 要查詢的年份。
            database (str | None): 資料表所在的資料庫名稱。

        Returns:
            pandas.DataFrame: 去重後的觀測點清單，含 `lat_round`、`lon_round` 兩欄。

        Raises:
            SQLAlchemyError: 查詢失敗。
        """
        return e_get_uniq_acc_geo(target_year, database=database)

    @task
    def task_prep_batch_plan(
        df_acc_unique_loc, target_year: int, batch_size: int
    ) -> list[dict]:
        """盤點還缺哪些「觀測點 × 月」，切成批次並存下各批清單。

        Args:
            df_acc_unique_loc (pandas.DataFrame): 去重後的觀測點清單。
            target_year (int): 要盤點的年份。
            batch_size (int): 幾個觀測點切成一批。

        Returns:
            list[dict]: 每個元素是一批的參數，供 `expand_kwargs()` 展開；
                空 list 代表該年度已全部抓完，是正常結果。

        Raises:
            GoogleAPIError: 盤點或寫入暫存檔失敗。
        """
        return prep_batch_plan(df_acc_unique_loc, target_year, batch_size)

    @task(
        pool="weather_api_pool",  # 需到UI進一步給值，指一次可以執行多少個同類task
        retries=3,  # 如果出現except，最多再重試3次，總計每個task可跑4次
        retry_delay=timedelta(minutes=40),  # 40分鐘後才重試
        retry_exponential_backoff=True,  # 讓等待時間隨次數增加(指數退避)
        max_retry_delay=timedelta(hours=3),  # 指數退避下，最長間隔3小時後重試
        # 排除等待時間，如果執行總時間超過2小時，殺掉該task避免佔用pool資源
        execution_timeout=timedelta(hours=2),
        do_xcom_push=False,  # 回傳的xcom不推送到下一個task，省掉存xcom的記憶體空間
    )
    def task_e_crawler_weatherapi(batch_id: int, target_year: int, month: int) -> str:
        """抓取一批觀測點在某個月的天氣觀測，逐點存成 GCS Parquet。

        以 `expand_kwargs()` 動態展開，一個實例就是一批觀測點乘一個月。task 設定
        走專屬的 pool 控管併發，重試採指數退避，因為 API 額度的限流窗口是分鐘到
        小時等級。

        Args:
            batch_id (int): 批號。
            target_year (int): 年份。
            month (int): 月份，1～12。

        Returns:
            str: 天氣資料所在的 GCS bucket 名稱。

        Raises:
            RuntimeError: 回傳筆數與請求的地點數不符，或寫入失敗率過高。
            requests.exceptions.RequestException: 請求失敗且重試耗盡（含額度用盡的 429）。
        """
        return e_crawler_weatherapi(batch_id, target_year, month)

    @task(
        retries=2,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=4),
        trigger_rule="all_done",
    )
    def task_t_and_l_weather(target_year: int, database: str | None) -> None:
        """讀取 GCS 上該年度的天氣觀測，分批清洗後寫入 `fact_hourly_weather`。

        觸發條件是上游全部結束（不論成敗），因為抓取被 API 額度打斷是這個設計的
        正常狀態，已經落地 GCS 的資料要先進 MySQL，不必等全部抓完。
        寫完後順帶回填 `fact_accident_main` 的 `weather_record_id`。

        Args:
            target_year (int): 要清洗並寫入的年份。
            database (str | None): 目標資料庫名稱。

        Raises:
            RuntimeError: 該年度在 GCS 上找不到任何檔案，或損壞檔案比例過高。
            pymysql.MySQLError: 寫入失敗。
        """
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
