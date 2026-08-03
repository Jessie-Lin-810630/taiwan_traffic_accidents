"""Transform 階段：自事故原始 CSV 清洗出事故類別維度 DataFrame。"""

import pandas as pd

from src.util.logger_crtx import get_logger
from src.util.read_traffic_accident_file import read_traffic_accident_file
from src.util.table_column_map import dim_accident_type_col_map

logger = get_logger(__name__)


def t_dim_accident_type(csvfile_paths: list[str]) -> pd.DataFrame:
    """這個函式的目的是從csv檔案中讀取交通事故資料，並且從中萃取出事故類型的維度表。

    這個維度表會包含每一種事故類型的唯一ID、名稱、描述等資訊，方便後續分析使用。
    :param csvfile_paths: 包含csv檔案路徑的列表，這些csv檔案是從政府資料開放平台爬取的交通事故資料。
    :return: 一個DataFrame，包含事故類型維度表的資料。
    """
    if not csvfile_paths:
        raise ValueError("csvfile_paths 為空，上游未產出任何 CSV 檔")

    all_df = []
    for file_path in csvfile_paths:
        logger.info(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案、挑欄、改名、去空白
        df = read_traffic_accident_file(file_path, dim_accident_type_col_map)

        # 去重
        single_df = df.drop_duplicates(
            subset=[
                "accident_category",
                "accident_position_major",
                "accident_position_minor",
                "accident_type_major",
                "accident_type_minor",
            ]
        )
        all_df.append(single_df)
        logger.info(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(single_df)}")

    df_dim_accident_type = pd.concat(all_df).drop_duplicates(
        subset=[
            "accident_category",
            "accident_position_major",
            "accident_position_minor",
            "accident_type_major",
            "accident_type_minor",
        ]
    )
    df_dim_accident_type = df_dim_accident_type.reset_index(drop=True, inplace=False)
    return df_dim_accident_type
