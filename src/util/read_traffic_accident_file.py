"""事故 CSV 的唯一讀取契約：讀檔、挑欄、改名、去字串空白（ADR-0010）。"""

from pathlib import Path

import pandas as pd

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def read_traffic_accident_file(
    csvfile_path: str | Path, column_map: dict[str, str]
) -> pd.DataFrame:
    """讀取一個 data.gov.tw 事故 CSV，挑出並改名成 column_map 指定的欄位。

    `skipfooter=2` 是對 data.gov.tw 檔案格式的斷言（末兩行是統計備註），
    `engine="python"` 是 `skipfooter` 的強制條件。

    欄位一律**按名稱**對應。CSV 缺少 column_map 中的某些欄位是允許的
    （來源會演進，例如「共享經濟或外送平台的名稱」是 114 年度才新增），
    缺席的欄位會成為 NaN 並留在正確的位置上，不會讓其後的欄位錯位。

    Parameters:
        csvfile_path: 事故 CSV 的路徑。
        column_map: 中文原始欄名 → 英文欄名的對照表。

    Returns:
        pd.DataFrame: 欄位為 column_map 的 value，順序與 column_map 一致。
    """
    df = pd.read_csv(csvfile_path, encoding="utf-8", skipfooter=2, engine="python")

    missing_columns = [c for c in column_map if c not in df.columns]
    if missing_columns:
        logger.warning(f"{csvfile_path} 缺少欄位 {missing_columns}，這些欄位將為 NaN")

    df = df.rename(columns=column_map)
    df = df.reindex(columns=list(column_map.values()))

    return df.map(lambda x: x.strip() if isinstance(x, str) else x)
