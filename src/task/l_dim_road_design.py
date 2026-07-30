"""Load 階段：將道路設計維度資料 upsert 進 `dim_road_design`。"""

import pandas as pd

from src.util.create_db_engine_or_database import get_pymysql_conn_to_mysql
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def l_dim_road_design(
    df_dim_road_design: pd.DataFrame, database: str | None = None
) -> None:
    """以 UPSERT 將道路設計維度資料寫入 `dim_road_design`。

    採用 INSERT ... ON DUPLICATE KEY UPDATE，發生錯誤時復原事務並原樣拋出例外。

    Parameters:
        df_dim_road_design (pandas.DataFrame): 待寫入的道路設計維度資料。
        database (str | None): 目標資料庫名稱。
    """
    # 準備INSERT資料表時需要的SQL語句，採用UPSERT
    # 先準備INSERT部分
    columns = ", ".join(df_dim_road_design.columns)
    placeholders = ", ".join(["%s"] * len(df_dim_road_design.columns))

    # 準備UPDATE的部分：故意只更新road_form_minor。
    update_part = "road_form_minor=VALUES(road_form_minor)"

    # 組合出完整SQL語句
    dml_str = f"""INSERT INTO dim_road_design ({columns})
                  VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_part};
                """
    # 6. 寫入資料表
    logger.info("====Inserting into table `dim_road_design`....====")
    conn = None
    cursor = None
    try:
        conn = get_pymysql_conn_to_mysql(database)
        if conn:
            cursor = conn.cursor()
            cursor.executemany(dml_str, df_dim_road_design.values.tolist())
            conn.commit()
    except Exception:
        logger.error("Error on inserting into table.", exc_info=True)
        if conn:
            conn.rollback()
        raise
    else:
        logger.info("====Successfully inserting into table `dim_road_design`====")
    finally:
        if conn:
            cursor.close()
            conn.close()
    return None
