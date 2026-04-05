from datetime import timedelta, datetime, timezone
from airflow.sdk import dag, task, TaskGroup
import os
from pathlib import Path
from airflow.models import Variable
from airflow.exceptions import AirflowException
from src.task.core.c_data_service import (get_and_slice_nightmarkets_multibatches,
                                          cal_accidents_nearby_nightmarket,
                                          aggregate_national_master)


# Default arguments for the DAG
default_args = {
    "owner": "jessie",  # DAG 擁有者名稱
    "depends_on_past": False,  # 任務是否依賴前一次DAG執行結果（False=獨立執行）
    "retries": 2,  # dag run失敗時最多重試2次，總計允許執行3次
    "retry_delay": timedelta(minutes=10),  # 除非task自己有額外定義，否則task重試需間隔10分鐘
}


@dag(
    dag_id="d06_precompute_to_redis",
    default_args=default_args,
    description="Precompute some tables/graph and save in REDIS database until TTL invalid",
    schedule="00 20 */5 * *",  # 每月1、6、11、16、21、26、31日的20點00分執行一次
    start_date=datetime(2026, 4, 4, 17, 00,
                        tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=['traffic', 'mart', 'taskflow'],
)
def precompute_to_redis():

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
    with TaskGroup(group_id="data_service_of_precompute_night_markets") as group:
        cal_done = task_cal_accidents_nearby_nightmarket.expand(batch_key=batch_keys_lst)
        agg_done = task_aggregate_national_master(batch_keys_lst)

        cal_done >> agg_done


precompute_to_redis()
