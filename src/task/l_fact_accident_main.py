import pandas as pd
from sqlalchemy import text
from src.util.create_db_engine_or_database import get_pymysql_conn_to_mysql
from airflow.models import Variable
from airflow.exceptions import AirflowException


def l_fact_accident_main(df_fact_accident_main: pd.DataFrame,
                         database: str | None = None) -> None:
    """"""
    # 準備INSERT資料表時需要的SQL語句，採用UPSERT
    # 先準備INSERT部分
    columns = ', '.join(df_fact_accident_main.columns)
    placeholders = ', '.join(['%s'] * len(df_fact_accident_main.columns))

    # 準備UPDATE的部分：故意只更新accident_time。
    update_part = "accident_time=VALUES(accident_time)"

    # 組合出完整SQL語句
    dml_str = f"""INSERT INTO fact_accident_main ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """

    # 6. 寫入資料表
    print(f"====Inserting into table `fact_accident_main`....====")
    conn = None
    cursor = None
    try:
        conn = get_pymysql_conn_to_mysql(database)
        if conn:
            cursor = conn.cursor()
            cursor.executemany(dml_str, df_fact_accident_main.values.tolist())
            conn.commit()
    except Exception as e:
        print(f"Error on inserting into table, Error msg: {e}")
        if conn:
            conn.rollback()
        raise AirflowException
    else:
        print(f"====Successfully inserting into table `fact_accident_main`====")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return None
