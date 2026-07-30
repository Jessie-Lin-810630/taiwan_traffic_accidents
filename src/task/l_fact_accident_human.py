"""Load 階段：將事故當事人事實資料 upsert 進 `fact_accident_human`。"""

import pandas as pd

from src.util.create_db_engine_or_database import get_pymysql_conn_to_mysql
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def l_fact_accident_human(
    df_fact_accident_human: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將事故當事人事實資料寫入 `fact_accident_human`。

    採用 INSERT ... ON DUPLICATE KEY UPDATE，發生錯誤時復原事務並原樣拋出例外。

    Parameters:
        df_fact_accident_human (pandas.DataFrame): 待寫入的事故當事人事實資料。
        database (str | None): 目標資料庫名稱。
    """
    # 準備INSERT資料表時需要的SQL語句，採用UPSERT
    # 先準備INSERT部分
    columns = ", ".join(df_fact_accident_human.columns)
    placeholders = ", ".join(["%s"] * len(df_fact_accident_human.columns))

    # 準備UPDATE的部分：故意只更新hit_and_run。
    update_part = "hit_and_run=VALUES(hit_and_run)"

    # 組合出完整SQL語句
    dml_str = f"""INSERT INTO fact_accident_human ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """
    # 6. 寫入資料表
    logger.info("====Inserting into table `fact_accident_human`....====")
    conn = None
    cursor = None
    try:
        conn = get_pymysql_conn_to_mysql(database)
        if conn:
            cursor = conn.cursor()
            cursor.executemany(dml_str, df_fact_accident_human.values.tolist())
            conn.commit()
    except Exception:
        logger.error("Error on inserting into table.", exc_info=True)
        if conn:
            conn.rollback()
        raise
    else:
        logger.info("====Successfully inserting into table `fact_accident_human`====")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return None
