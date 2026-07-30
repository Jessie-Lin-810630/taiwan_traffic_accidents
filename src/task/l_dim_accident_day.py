"""Load 階段：將事故日維度資料 upsert 進 `dim_accident_day`。"""

import pandas as pd

from src.util.create_db_engine_or_database import get_pymysql_conn_to_mysql
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def l_dim_accident_day(
    df_dim_accident_day: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將事故日維度資料寫入 `dim_accident_day`。

    採用 INSERT ... ON DUPLICATE KEY UPDATE，發生錯誤時復原事務並原樣拋出例外。

    Parameters:
        df_dim_accident_day (pandas.DataFrame): 待寫入的事故日維度資料。
        database (str | None): 目標資料庫名稱。
    """
    # 準備INSERT資料表時需要的SQL語句，採用UPSERT
    # 先準備INSERT部分
    columns = ", ".join(df_dim_accident_day.columns)
    placeholders = ", ".join(["%s"] * len(df_dim_accident_day.columns))

    # 準備UPDATE的部分：故意只更新accident_weekday。
    update_part = "accident_weekday=VALUES(accident_weekday)"

    # 組合出完整SQL語句
    dml_str = f"""INSERT INTO dim_accident_day ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """
    # 6. 寫入資料表
    logger.info("====Inserting into table `dim_accident_weekday`....====")
    conn = None
    cursor = None
    try:
        conn = get_pymysql_conn_to_mysql(database)
        if conn:
            cursor = conn.cursor()
            cursor.executemany(dml_str, df_dim_accident_day.values.tolist())
            conn.commit()
    except Exception:
        logger.error("Error on inserting into table.", exc_info=True)
        if conn:
            conn.rollback()
        raise
    else:
        logger.info("====Successfully inserting into table `dim_accident_weekday`====")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return None
