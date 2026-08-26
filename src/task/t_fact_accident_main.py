"""Transform 階段：自事故原始 CSV 清洗出事故主檔事實 DataFrame。"""

import hashlib

import numpy as np
import pandas as pd

from src.util.logger_crtx import get_logger
from src.util.mysql_utils import get_table_from_sqlserver
from src.util.read_traffic_accident_file import read_traffic_accident_file
from src.util.table_column_map import fact_accident_main_col_origin_map

logger = get_logger(__name__)


def t_fact_accident_main(csvfile_paths: list[str], database: str) -> pd.DataFrame:
    """從事故 CSV 清洗出事故主檔事實資料，一件事故一列。

    逐檔讀入後做四件清洗：日期轉成 `YYYY-MM-DD`、時間補零並轉成 `HH:MM:SS`、
    把「死亡X;受傷Y」拆成兩個整數欄、經緯度轉成浮點數。合併所有檔案後，回頭
    查 `dim_accident_day` 與 `dim_accident_type` 兩張維度表取得外鍵，因此執行前
    這兩張維度表必須已經載入。

    主鍵 `accident_id` 由「日期八碼 + 日期、時間、經緯度四欄的 SHA-256 前 16 碼」
    組成，共 24 碼。這四欄就是資料表的唯一鍵，也是去重的依據，因此同一件事故
    無論和哪些資料一起被處理，都會得到同一個編號 —— 來源在既有日期補登事故時
    不會讓其他事故換號。

    Args:
        csvfile_paths (list[str]): 事故 CSV 的路徑清單，來自 `e_*` 階段的產出。
        database (str): 要查詢維度表的資料庫名稱，由呼叫端指定。

    Returns:
        pandas.DataFrame: 事故主檔資料，空值已轉成 `None` 以便寫入 MySQL，形如：

            accident_id              accident_type_id  day_id  accident_time  death_count  injury_count  longitude   latitude
            202401019b7e40aa3f2ac81d  12                1       08:15:00       0            1             121.552300  25.088100
            2024010153c8b1f70d9e26ba  47                1       09:40:00       1            0             120.658700  24.152600

    Raises:
        ValueError: `csvfile_paths` 為空，代表上游沒有產出任何 CSV。
        FileNotFoundError: 清單中的某個路徑不存在。
        SQLAlchemyError: 查詢維度表失敗。

    Notes:
        空清單視為故障參考 ADR-0003，中途查維度表取外鍵是刻意的設計，參考 ADR-0010，
        主鍵由事故內容決定而非單次 run 的排序名次參考 ADR-0014，
        資料庫名稱由呼叫端傳入參考 ADR-0018。
    """
    if not csvfile_paths:
        raise ValueError("csvfile_paths 為空，上游未產出任何 CSV 檔")

    all_df = []
    for file_path in csvfile_paths:
        logger.info(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案、挑欄、改名、去空白
        df = read_traffic_accident_file(file_path, fact_accident_main_col_origin_map)

        # 清理發生日期
        df["accident_date"] = pd.to_datetime(
            df["accident_date"], errors="coerce", format="%Y%m%d"
        )
        df["accident_date"] = df["accident_date"].astype(str)

        # 清理發生時間
        df["accident_time"] = df["accident_time"].astype(str).str.zfill(6)
        df["accident_time"] = df["accident_time"].apply(
            lambda r: r[0:2] + ":" + r[2:4] + ":" + r[4:]
        )
        # 清理死傷人數
        df["death_count"] = df["casualties_count"].apply(
            lambda r: int(r.split(";")[0].replace("死亡", ""))
        )
        df["injury_count"] = df["casualties_count"].apply(
            lambda r: int(r.split(";")[1].replace("受傷", ""))
        )

        # 清理經緯度
        df["longitude"] = df["longitude"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")

        all_df.append(df)
        logger.info(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(df)}")

    # union
    df = pd.concat(all_df)

    # 找day_id關聯
    query = "SELECT day_id, accident_date FROM dim_accident_day;"
    df_dim_accident_day = get_table_from_sqlserver(query, database=database)
    df_dim_accident_day["accident_date"] = df_dim_accident_day["accident_date"].astype(
        str
    )

    df_merged = df.merge(
        df_dim_accident_day,
        how="inner",
        left_on="accident_date",
        right_on="accident_date",
    )

    # 找accident_type_id關聯
    query = "SELECT * FROM dim_accident_type;"
    df_dim_accident_type = get_table_from_sqlserver(query, database=database)
    df_merged = df_merged.merge(
        df_dim_accident_type,
        how="inner",
        left_on=[
            "accident_category",
            "accident_position_major",
            "accident_position_minor",
            "accident_type_major",
            "accident_type_minor",
        ],
        right_on=[
            "accident_category",
            "accident_position_major",
            "accident_position_minor",
            "accident_type_major",
            "accident_type_minor",
        ],
    )

    # 去重
    df_fact_accident_main = df_merged.drop_duplicates(
        subset=["day_id", "accident_time", "longitude", "latitude"]
    ).reset_index(drop=True)

    # 生成PK (YYYYMMDD + 唯一鍵四欄的SHA-256前16碼)
    # 經緯度先格式化成固定6位小數，否則同一筆事故在不同次run會算出不同的雜湊
    uk = (
        df_fact_accident_main["accident_date"].astype(str)
        + "|"
        + df_fact_accident_main["accident_time"].astype(str)
        + "|"
        + df_fact_accident_main["longitude"].map(lambda v: f"{v:.6f}")
        + "|"
        + df_fact_accident_main["latitude"].map(lambda v: f"{v:.6f}")
    )
    df_fact_accident_main["accident_id"] = df_fact_accident_main[
        "accident_date"
    ].astype(str).str.replace("-", "") + uk.apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()[:16]
    )

    # 留下想要的欄位
    df_fact_accident_main = df_fact_accident_main.loc[
        :,
        [
            "accident_id",
            "accident_type_id",
            "day_id",
            "accident_time",
            "death_count",
            "injury_count",
            "longitude",
            "latitude",
        ],
    ]

    # 填補空值，將NaN轉換成None
    df_fact_accident_main = df_fact_accident_main.replace({np.nan: None})

    return df_fact_accident_main
