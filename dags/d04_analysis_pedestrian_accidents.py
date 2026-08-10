"""DAG d04：依序執行 mart 層 SQL，重建行人事故分析用的資料表。

每月 15 日 13 點跑一次。mart 層資料表是前端各頁面的資料來源，因此本 DAG 要排在
事故資料載入完成之後。SQL 腳本放在 `src/task/mart_table_sql/`，一個檔案含多條
敘述、整份一次執行，任一檔失敗則整批復原。
"""

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
    """串接兩個 task：先找出 mart 層 SQL 檔案，再依序執行。"""

    @task
    def task_find_sql_files(sql_files_dir):
        """遞迴蒐集目錄底下所有 `.sql` 檔案的路徑。

        Args:
            sql_files_dir (str | Path): mart 層 SQL 腳本目錄。

        Returns:
            list[str]: 找到的 `.sql` 檔案路徑，交由下游 task 執行。

        Raises:
            FileNotFoundError: 目錄底下沒有任何 `.sql` 檔案。
        """
        return find_sql_files(sql_files_dir)

    @task
    def task_exec_mart_sql_files(sql_file_paths, database):
        """依序執行所有 mart 層 SQL 檔案，全數成功後才一次提交。

        Args:
            sql_file_paths (list[str]): 要執行的 `.sql` 檔案路徑。
            database (str): 目標資料庫名稱。

        Returns:
            None: 本 task 只有副作用。

        Raises:
            pymysql.MySQLError: 任一檔案執行失敗，整批事務復原後往外拋。
        """
        exec_mart_sql_files(sql_file_paths, database)
        return None

    database = os.getenv("MYSQL_DATABASE")
    sql_file_path_lst = task_find_sql_files(MART_SQL_DIR)
    task_exec_mart_sql_files(sql_file_path_lst, database)


analysis_pedestrian_accidents()
