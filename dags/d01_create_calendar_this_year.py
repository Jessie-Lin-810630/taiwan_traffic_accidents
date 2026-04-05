from datetime import timedelta, datetime, timezone
from airflow.sdk import dag, task, TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
import os
from src.task.create_traffic_accident_tables import create_traffic_accident_tables, create_engine_to_mysql, create_database
from src.task.t_dim_accident_day import t_data_for_dim_accident_day, taiwan_national_activities
from src.task.l_dim_accident_day import l_dim_accident_day


# Default arguments for the DAG
default_args = {
    "owner": "jessie",  # DAG 擁有者名稱
    "depends_on_past": False,  # 任務是否依賴前一次DAG執行結果（False=獨立執行）
    "retries": 2,  # dag run失敗時最多重試2次，總計允許執行3次
    "retry_delay": timedelta(minutes=10),  # 除非task自己有額外定義，否則task重試需間隔10分鐘
}


@dag(
    dag_id="d01_create_calendar_this_year",
    default_args=default_args,
    description="A computing process adding calendar in database",
    schedule=None,
    start_date=None,
    catchup=False,
    tags=['traffic', 'taskflow'],
)
def calendar_pipeline():
    database = os.getenv("MYSQL_DATABASE")

    @task
    def task_crx_database_and_table(database):
        engine = create_engine_to_mysql(database)
        create_database(engine, database)
        create_traffic_accident_tables(engine)
        return None

    @task(retries=3, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(minutes=30))
    def task_t_and_l_day(database):
        start = datetime(2021, 1, 1, tzinfo=timezone(timedelta(hours=8)))  # 台灣時區
        end = datetime(2026, 12, 31, tzinfo=timezone(timedelta(hours=8)))

        t_done_day = t_data_for_dim_accident_day(start, end, taiwan_national_activities)
        l_dim_accident_day(t_done_day, database)
        return None

    crx_done = task_crx_database_and_table(database)
    tl_done = task_t_and_l_day(database)
    crx_done >> tl_done


# Instantiate the DAG
calendar_pipeline()
