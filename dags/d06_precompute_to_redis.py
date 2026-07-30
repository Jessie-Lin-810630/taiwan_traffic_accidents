"""DAG d06：預先計算夜市周邊事故統計並寫入 Redis 快取。"""

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
    """分批計算夜市周邊事故，再彙總為全國主檔並存入 Redis。"""

    @task
    def task_get_and_slice_nm_multibatches():
        market_batch_keys = get_and_slice_nightmarkets_multibatches()
        return market_batch_keys

    @task
    def task_cal_accidents_nearby_nightmarket(batch_key):
        cal_result = cal_accidents_nearby_nightmarket(batch_key)
        return cal_result

    @task
    def task_aggregate_national_master(market_batch_keys):
        aggregate_national_master(market_batch_keys)
        return None

    batch_keys_lst = task_get_and_slice_nm_multibatches()
    with TaskGroup(group_id="data_service_of_precompute_night_markets"):
        cal_done = task_cal_accidents_nearby_nightmarket.expand(
            batch_key=batch_keys_lst
        )
        agg_done = task_aggregate_national_master(batch_keys_lst)

        cal_done >> agg_done


precompute_to_redis()
