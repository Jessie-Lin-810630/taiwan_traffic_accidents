"""Transform 階段：自事故原始 CSV 清洗出道路設計維度 DataFrame。"""

import pandas as pd

from src.util.logger_crtx import get_logger
from src.util.table_column_map import dim_road_design_col_map

logger = get_logger(__name__)


def t_dim_road_design(csvfile_paths: list[str]) -> pd.DataFrame:
    """這個函式的目的是從csv檔案中讀取交通事故資料，並且從中萃取出車道設計的維度表。

    這個維度表會包含每一種車道類型的唯一ID、名稱、描述等資訊，方便後續分析使用。
    :param csvfile_paths: 包含csv檔案路徑的列表，這些csv檔案是從政府資料開放平台爬取的交通事故資料。
    :return: 一個DataFrame，包含車道設計維度表的資料。
    """
    all_df = []
    for file_path in csvfile_paths:
        logger.info(f"正在處理csv檔案: {file_path}")
        # 讀取csv檔案到DataFrame
        df = pd.read_csv(file_path, encoding="utf-8", skipfooter=2, engine="python")

        # 擷取需要的欄位
        required_columns = [k for k in dim_road_design_col_map.keys()]
        df = df.loc[:, required_columns]

        # 重新命名欄位
        renamed_required_columns = [
            dim_road_design_col_map[k] for k in required_columns
        ]
        df.columns = renamed_required_columns

        # 去空白
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        # 去重
        single_df = df.drop_duplicates(
            subset=["road_type_primary_party", "road_form_major", "road_form_minor"]
        )
        all_df.append(single_df)
        logger.info(f"成功讀取csv檔案: {file_path}。此輪得到列數: {len(single_df)}")

    if all_df:
        df_dim_road_design = pd.concat(all_df).drop_duplicates(
            subset=["road_type_primary_party", "road_form_major", "road_form_minor"]
        )
        df_dim_road_design = df_dim_road_design.reset_index(drop=True, inplace=False)
        return df_dim_road_design
    return pd.DataFrame()
