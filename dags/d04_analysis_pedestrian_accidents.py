"""DAG d04：依序執行 mart 層 SQL，重建行人事故分析用的資料表。"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task

from src.util.create_db_engine_or_database import (
    get_pymysql_conn_to_mysql_multistatement,
)
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

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
    def find_sql_files(sql_files_dir: str | Path) -> list[str]:
        if isinstance(sql_files_dir, str):
            sql_files_dir = Path(sql_files_dir)

        sql_file_paths = [str(f) for f in sql_files_dir.rglob("*.sql")]
        if not sql_file_paths:
            raise FileNotFoundError(
                f".sql files not found in the directory {sql_files_dir}!"
            )
        return sql_file_paths

    def exec_sql_linebyline(sql_str: str, database: str) -> None:
        try:
            conn = get_pymysql_conn_to_mysql_multistatement(database)
            cursor = conn.cursor()
            logger.info(f"type of sql_str: {type(sql_str)}")
            cursor.execute(sql_str)
        except Exception:
            logger.error("SQL執行失敗", exc_info=True)
            if conn:
                conn.rollback()
            raise
        else:
            logger.info("Mart層資料表建立成功!")
        finally:
            cursor.close()
            conn.close()
            return None

    @task()
    def read_sql(sql_file_paths: list[str]) -> list[str]:
        database = os.getenv("MYSQL_DATABASE")
        conn = get_pymysql_conn_to_mysql_multistatement(database)
        cursor = conn.cursor()
        file_path = None  # 供 except 區塊指出失敗的檔案，避免引用未綁定的迴圈變數
        try:
            for i in range(len(sql_file_paths)):
                file_path = sql_file_paths[i]
                logger.info(f"正在處理第{i + 1}份: {os.path.basename(file_path)}")
                with open(file_path, mode="r") as f:
                    sql_content = f.read()
                cursor.execute(sql_content)

                # 有可能資料庫可能還沒真正完成報錯，但Python認為已經跑完了而提前印出"建立成功"。
                # 這裡要強制用python檢查所有result sets都有消耗掉，才可以離開while loop進入下一行。
                while conn.next_result():
                    pass
                logger.info("Mart層資料表建立成功!")
                # # 使用 sqlparse 移除註解並格式化
                # clean_sql = sqlparse.format(sql_content, strip_comments=True)

                # # 分割成語句列表（sqlparse會自動處理分號）
                # list_of_sql_statements = sqlparse.split(clean_sql)

                # # 移除空語句
                # list_of_sql_statements = [stmt.strip() for stmt in list_of_sql_statements
                #                           if stmt.strip()]
                # print(f"去除註解後、清理SQL語句數量: {len(list_of_sql_statements)}")

                # # 開始執行
                # exec_sql_linebyline(list_of_sql_statements)
            logger.info("全數sql file解析且執行完成!")
        except Exception:
            logger.error(f"處理 sql file 失敗: {file_path}", exc_info=True)
            if conn:
                conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
        return None

    sql_file_path_lst = find_sql_files(Path().resolve() / "src/task/mart_table_sql")
    read_sql(sql_file_path_lst)


analysis_pedestrian_accidents()
