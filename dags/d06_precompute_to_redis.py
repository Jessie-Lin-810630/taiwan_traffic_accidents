"""DAG d06：預先計算夜市周邊事故統計並寫入 Redis 快取。

每五天的 20 點跑一次。前端頁面只讀這裡算好的結果，不自行做重運算，因此本 DAG
沒跑完的話，各分頁會顯示「請確認排程是否執行完成」。產出的快取存活 12 小時到
10 天不等。
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import TaskGroup, dag, task

from src.task.core.c_data_service import (
    aggregate_national_master,
    cal_accidents_nearby_nightmarket,
    get_and_slice_nightmarkets_multibatches,
)

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
    dag_id="d06_precompute_to_redis",
    default_args=default_args,
    description="Precompute some tables/graph and save in REDIS database until TTL invalid",
    schedule="00 20 */5 * *",  # 每月1、6、11、16、21、26、31日的20點00分執行一次
    start_date=datetime(2026, 4, 4, 17, 00, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=["traffic", "mart", "taskflow"],
)
def precompute_to_redis():
    """串接三個 task：切批次、逐批計算周邊事故、彙總成全臺總表。

    中間那個 task 以動態展開的方式，每個批次各跑一次。
    """

    @task
    def task_get_and_slice_nm_multibatches():
        """把夜市清單切成批次存進 Redis，回傳各批的鍵。

        Returns:
            list[str]: 各批次在 Redis 中的鍵，供下游 task 展開與彙總。

        Raises:
            RedisError: 讀取或寫入快取失敗。
            SQLAlchemyError: 查詢 MySQL 失敗。
        """
        market_batch_keys = get_and_slice_nightmarkets_multibatches()
        return market_batch_keys

    @task
    def task_cal_accidents_nearby_nightmarket(market_batch_key):
        """計算一個批次內每個夜市周邊的事故，結果寫入 Redis。

        由 `expand()` 依批次鍵動態展開，每個批次各是一個 task 實例。

        Args:
            market_batch_key (str): 該批次夜市清單在 Redis 中的鍵。

        Returns:
            str: 處理完成的訊息。

        Raises:
            ValueError: 批次在 Redis 中不存在或為空。
            RedisError: 讀取或寫入快取失敗。
        """
        cal_result = cal_accidents_nearby_nightmarket(market_batch_key)
        return cal_result

    @task
    def task_aggregate_national_master(market_batch_keys):
        """彙總各批次的計算結果成全臺總表與儀表板統計，存入 Redis。

        Args:
            market_batch_keys (list[str]): 各批次在 Redis 中的鍵。

        Returns:
            None: 本 task 只有副作用。

        Raises:
            RuntimeError: 所有批次皆找不到周邊事故快取，聚合不出全臺總表。
            RedisError: 讀取或寫入快取失敗。
        """
        aggregate_national_master(market_batch_keys)
        return None

    batch_keys_lst = task_get_and_slice_nm_multibatches()
    with TaskGroup(group_id="data_service_of_precompute_night_markets"):
        cal_done = task_cal_accidents_nearby_nightmarket.expand(
            market_batch_key=batch_keys_lst
        )
        agg_done = task_aggregate_national_master(batch_keys_lst)

        cal_done >> agg_done


precompute_to_redis()
