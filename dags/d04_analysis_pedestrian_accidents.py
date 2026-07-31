"""DAG d04：依序執行 mart 層 SQL，重建行人事故分析用的資料表。"""

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from src.task.exec_mart_sql import exec_mart_sql_files, find_sql_files
from src.util.paths import MART_SQL_DIR

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
    dag_id="d04_analysis_pedestrian_accidents",
    default_args=default_args,
    description="Analysis works and refresh the Mart tables in MySQL database",
    schedule="00 13 15 * *",  # 每月15日的13點00分執行一次
    start_date=datetime(2026, 4, 4, 17, 00, tzinfo=timezone(offset=timedelta(hours=8))),
    catchup=False,
    tags=["traffic", "mart", "taskflow"],
)
def analysis_pedestrian_accidents():
    """蒐集 mart 層 SQL 檔案後依序執行，重建分析用資料表。"""

    @task
    def task_find_sql_files(sql_files_dir):
        return find_sql_files(sql_files_dir)

    @task
    def task_exec_mart_sql_files(sql_file_paths, database):
        exec_mart_sql_files(sql_file_paths, database)
        return None

    database = os.getenv("MYSQL_DATABASE")
    sql_file_path_lst = task_find_sql_files(MART_SQL_DIR)
    task_exec_mart_sql_files(sql_file_path_lst, database)


analysis_pedestrian_accidents()
