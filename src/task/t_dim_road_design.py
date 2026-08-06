"""Transform 階段：自事故原始 CSV 清洗出道路設計維度 DataFrame。"""

import pandas as pd

from src.util.logger_crtx import get_logger
from src.util.read_traffic_accident_file import read_traffic_accident_file
from src.util.table_column_map import dim_road_design_col_map

logger = get_logger(__name__)


def t_dim_road_design(csvfile_paths: list[str]) -> pd.DataFrame:
    """從事故 CSV 萃取出道路設計維度資料，一種道路設計一列。

    逐檔讀入後先各自去重，合併再去重一次，因此跨年度重複出現的道路設計只會
    留下一列。去重的依據是道路類別與道路型態大小類三欄的組合，與資料表的
    唯一鍵一致。傳入空清單視為上游故障而拋出，不是回傳空 DataFrame。

    Args:
        csvfile_paths (list[str]): 事故 CSV 的路徑清單，來自 `e_*` 階段的產出。

    Returns:
        pandas.DataFrame: 去重後的道路設計，索引已重設，形如：

            road_type_primary_party  road_form_major  road_form_minor
            市區道路                 交岔路           四岔路
            省道                     直路             一般直路

    Raises:
        ValueError: `csvfile_paths` 為空，代表上游沒有產出任何 CSV。
        FileNotFoundError: 清單中的某個路徑不存在。

    Notes:
        空清單視為故障參考 ADR-0003，CSV 讀取契約參考 ADR-0010。
    """
    if not csvfile_paths:
        raise ValueError("csvfile_paths 為空，上游未產出任何 CSV 檔")

    all_df = []
    for file_path in csvfile_paths:
        logger.info(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案、挑欄、改名、去空白
        df = read_traffic_accident_file(file_path, dim_road_design_col_map)

        # 去重
        single_df = df.drop_duplicates(
            subset=["road_type_primary_party", "road_form_major", "road_form_minor"]
        )
        all_df.append(single_df)
        logger.info(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(single_df)}")

    df_dim_road_design = pd.concat(all_df).drop_duplicates(
        subset=["road_type_primary_party", "road_form_major", "road_form_minor"]
    )
    df_dim_road_design = df_dim_road_design.reset_index(drop=True, inplace=False)
    return df_dim_road_design
