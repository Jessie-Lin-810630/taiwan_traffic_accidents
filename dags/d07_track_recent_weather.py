"""DAG d07：抓取本年度至今的天氣觀測，並載入 MySQL。

每月 3 號與 18 號的 07:00 各跑一次：3 號抓剛完成的上個月（該月最後一天已進入
API 查得到的範圍，因此判定為抓完整、之後不再重抓），18 號抓當月 1 到 15 日。

頻率的依據是事故資料兩週發布一次 —— 天氣是掛在事故上的，抓得比事故新沒有意義。
四個 task 串成：取觀測點 → 制訂批次 → 分批抓取 → 清洗載入。歷年的回補在 d08。
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
    """串接本年度的天氣 ETL：取觀測點、制訂批次、分批抓取、清洗載入。"""
    this_year = 2026
    database = os.getenv("MYSQL_DATABASE")

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

        Notes:
            回傳值設計，參考 ADR-00013。
        """
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

        Args:
            target_year (int): 要清洗並寫入的年份。
            database (str | None): 目標資料庫名稱。

        Raises:
            RuntimeError: 該年度在 GCS 上找不到任何檔案，或損壞檔案比例過高。
            pymysql.MySQLError: 寫入失敗。

        Notes:
            trigger_rule="all_done" 參考 ADR-00013。
        """
        l_fact_hourly_weather(target_year, database=database, batch_size=50)

    with TaskGroup(group_id=f"year_{this_year}"):
        df = task_e_get_uniq_acc_geo(this_year, database)
        batches = task_prep_batch_plan(df, this_year, 50)
        # MappedOperator。一個 mapped instance = 一批觀測點 × 一個月，
        craw_done = task_e_crawler_weatherapi.expand_kwargs(batches)

        load_done = task_t_and_l_weather(this_year, database)
        craw_done >> load_done


# Instantiate the DAG
accident_weather_pipeline()
