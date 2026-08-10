"""Transform 階段：自事故原始 CSV 清洗出事故當事人事實 DataFrame。"""

import hashlib

import numpy as np
import pandas as pd

from src.util.logger_crtx import get_logger
from src.util.mysql_utils import get_table_from_sqlserver
from src.util.read_traffic_accident_file import read_traffic_accident_file
from src.util.table_column_map import fact_accident_human_col_origin_map

logger = get_logger(__name__)


def t_fact_accident_human(csvfile_paths: list[str]) -> pd.DataFrame:
    """從事故 CSV 清洗出事故當事人事實資料，一位當事人一列。

    逐檔讀入後清洗日期、時間與經緯度，並做三項當事人專屬的處理：性別不是男或女
    時年齡填 -1（那類列的年齡欄實際上是物件而非人）、肇逃轉成 0 或 1、依當事者
    順位標出是否為第一肇事者。合併所有檔案後查 `dim_accident_day` 與
    `fact_accident_main` 取得 `accident_id`，因此執行前那兩張表必須已經載入。

    每列另外算一個 `row_hash`，取事故編號、當事者順位、年齡、性別、肇因子類別與
    其他撞擊部位六欄湊成字串後取 SHA-256 前 32 碼，作為資料表的唯一鍵。函式會把
    重複的 hash 筆數記進日誌，供檢查這組欄位是否足以區分不同當事人。

    來源 CSV 若沒有「共享經濟或外送平台的名稱」欄（舊年度表頭），該欄會是 NaN，
    不影響其餘欄位。

    Args:
        csvfile_paths (list[str]): 事故 CSV 的路徑清單，來自 `e_*` 階段的產出。

    Returns:
        pandas.DataFrame: 當事人資料，空值已轉成 `None` 以便寫入 MySQL，形如：

            accident_id      party_sequence  is_primary_party_sequence  gender  age  vehicle_type_major  hit_and_run  row_hash
            2024010100000001  1               1                          男      34   機車                0            3f2a...c81d
            2024010100000001  2               0                          女      28   自用小客車          0            9b7e...40aa

    Raises:
        ValueError: `csvfile_paths` 為空，代表上游沒有產出任何 CSV；
            或有列在 `fact_accident_main` 找不到對應的事故，代表主檔缺漏或錯亂。
        FileNotFoundError: 清單中的某個路徑不存在。
        SQLAlchemyError: 查詢維度表或事故主檔失敗。

    Notes:
        空清單視為故障參考 ADR-0003，中途查維度表取外鍵是刻意的設計，參考 ADR-0010，
        對不到主檔就地 raise 參考 ADR-0014。
    """
    if not csvfile_paths:
        raise ValueError("csvfile_paths 為空，上游未產出任何 CSV 檔")

    all_df = []
    for file_path in csvfile_paths:
        logger.info(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案、挑欄、改名、去空白
        # 舊表頭沒有「共享經濟或外送平台的名稱」，該欄會是 NaN
        df = read_traffic_accident_file(file_path, fact_accident_human_col_origin_map)

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
        # 清理經緯度
        df["longitude"] = df["longitude"].astype("float64")
        df["latitude"] = df["latitude"].astype("float64")

        # 清理age
        df["age"] = np.where(df["gender"].isin(["男", "女"]), df["age"], -1)

        # 清理肇逃
        df["hit_and_run"] = df["hit_and_run"].apply(lambda r: 1 if r == "是" else 0)

        # 建立is_primary_party_sequence欄位
        df["is_primary_party_sequence"] = None
        df["party_sequence"] = df["party_sequence"].astype("int64")
        df["is_primary_party_sequence"] = np.where(df["party_sequence"].eq(1), 1, 0)

        all_df.append(df)
        logger.info(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(df)}")

    # union
    df = pd.concat(all_df)

    # 找day_id關聯
    query = "SELECT day_id, accident_date FROM dim_accident_day;"
    df_dim_accident_day = get_table_from_sqlserver(query, database="traffic_accidents")
    df_dim_accident_day["accident_date"] = df_dim_accident_day["accident_date"].astype(
        str
    )
    df_merged = df.merge(
        df_dim_accident_day,
        how="left",
        left_on="accident_date",
        right_on="accident_date",
    )

    # 找accident_id關聯
    query = """SELECT accident_id, day_id, accident_time, longitude, latitude
                    FROM fact_accident_main;"""
    df_fact_accident_main = get_table_from_sqlserver(
        query, database="traffic_accidents"
    )
    df_fact_accident_main["accident_time"] = (
        df_fact_accident_main["accident_time"]
        .astype(str)
        .str.replace("0 days", "")
        .str.strip()
    )
    df_fact_accident_main["longitude"] = df_fact_accident_main["longitude"].astype(
        "float64"
    )
    df_fact_accident_main["latitude"] = df_fact_accident_main["latitude"].astype(
        "float64"
    )
    df_merged = df_merged.merge(
        df_fact_accident_main,
        how="left",
        left_on=["day_id", "accident_time", "longitude", "latitude"],
        right_on=["day_id", "accident_time", "longitude", "latitude"],
    )

    # 對不到主檔代表主檔缺漏或錯亂，在這裡停下來才說得出是哪幾筆對不上
    unmatched = df_merged["accident_id"].isna()
    if unmatched.any():
        sample = df_merged.loc[
            unmatched, ["day_id", "accident_time", "longitude", "latitude"]
        ].head(5)
        raise ValueError(
            f"有 {unmatched.sum()} 列在 fact_accident_main 找不到對應的事故，"
            f"前 5 筆對不上的鍵：\n{sample}"
        )

    # 生成row_hash
    uk = (
        df_merged["accident_id"].astype(str)
        + "|"
        + df_merged["party_sequence"].astype(str)
        + "|"
        + df_merged["age"].astype(str)
        + "|"
        + df_merged["gender"].astype(str)
        + "|"
        + df_merged["cause_analysis_minor_individual"].astype(str)
        + "|"
        + df_merged["impact_point_minor_other"].astype(str)
    )
    df_merged["row_hash"] = uk.apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()[:32]
    )

    # 留著檢查row_hash設計是否足夠確保業務唯一性
    dup_cnt = df_merged["row_hash"].duplicated().sum()
    logger.info(
        f"There are {dup_cnt} rows having duplicated combination of accident_id, party_sequence,"
        f"age, gender, cause_analysis_minor_individual and impact_point_minor_other."
    )

    # 留下想要的欄位
    df_fact_accident_human = df_merged.loc[
        :,
        [
            "accident_id",
            "party_sequence",
            "is_primary_party_sequence",
            "gender",
            "age",
            "protective_equipment",
            "mobile_device_usage",
            "party_action_major",
            "party_action_minor",
            "vehicle_type_major",
            "vehicle_type_minor",
            "cause_analysis_major_individual",
            "cause_analysis_minor_individual",
            "serving_sharing_economy_or_delivery",
            "impact_point_major_initial",
            "impact_point_minor_initial",
            "impact_point_major_other",
            "impact_point_minor_other",
            "hit_and_run",
            "row_hash",
        ],
    ]

    # 填補空值，將NaN轉換成None
    df_fact_accident_human = df_fact_accident_human.replace({np.nan: None})

    return df_fact_accident_human
