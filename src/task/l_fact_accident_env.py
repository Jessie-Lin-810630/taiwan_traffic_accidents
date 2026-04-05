import pandas as pd
from sqlalchemy import text
from src.util.create_db_engine_or_database import get_pymysql_conn_to_mysql
from airflow.models import Variable
from airflow.exceptions import AirflowException


def l_fact_accident_env(df_fact_accident_env: pd.DataFrame,
                        database: str | None = None) -> None:
    """"""
    # 準備INSERT資料表時需要的SQL語句，採用UPSERT
    # 先準備INSERT部分
    columns = ', '.join(df_fact_accident_env.columns)
    placeholders = ', '.join(['%s'] * len(df_fact_accident_env.columns))

    # 準備UPDATE的部分：故意只更新weather_condition。
    update_part = "weather_condition=VALUES(weather_condition)"

    # 組合出完整SQL語句
    dml_str = f"""INSERT INTO fact_accident_env ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """

    # 6. 寫入資料表
    print(f"====Inserting into table `fact_accident_env`....====")
    conn = None
    cursor = None
    try:
        conn = get_pymysql_conn_to_mysql(database)
        if conn:
            cursor = conn.cursor()
            cursor.executemany(dml_str, df_fact_accident_env.values.tolist())
            conn.commit()
    except Exception as e:
        print(f"Error on inserting into table, Error msg: {e}")
        if conn:
            conn.rollback()
        raise AirflowException
    else:
        print(f"====Successfully inserting into table `fact_accident_env`====")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return None
