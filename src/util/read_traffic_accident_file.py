"""車禍事故 CSV 的讀取與初步清理邏輯，含讀檔、挑欄、改名、去除字串前後空白。

六支讀事故 CSV 的 `t_*.py` 一律先經由本模組的函式讀 CSV 檔。

Notes:
    參考 ADR-0010。
"""

from pathlib import Path

import pandas as pd

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def read_traffic_accident_file(
    csvfile_path: str | Path, column_map: dict[str, str]
) -> pd.DataFrame:
    """讀取一個 data.gov.tw 事故 CSV，挑出並改名成 `column_map` 指定的欄位。

    讀檔時會跳過檔案中末兩行（因為根據專案情境，來源檔案固定以兩行統計備註結尾），
    並在讀完後把所有字串欄位的前後空白去除。

    欄位一律按名稱對應，不按位置。CSV 缺少 `column_map` 中的某些欄位是允許的
    ——資料來源會逐年演進，例如「共享經濟或外送平台的名稱」是 114 年度才新增
    ——缺席的欄位只記 warning，值填 NaN 並留在正確的位置上，不會讓其後的欄位錯位。

    Args:
        csvfile_path (str | Path): 事故 CSV 的路徑。
        column_map (dict[str, str]): 中文原始欄名對應英文欄名的對照表，
            定義在 `src/util/table_column_map.py`。

    Returns:
        pandas.DataFrame: 欄位為 `column_map` 的值、順序與 `column_map` 一致，形如：

            accident_id  city    occurred_at          deaths  injuries
            1130101001   臺北市  2024-01-01 08:15:00  0       1
            1130101002   新北市  2024-01-01 09:40:00  1       0

    Raises:
        FileNotFoundError: 路徑不存在。
        pandas.errors.ParserError: 檔案不是合法的 CSV 或欄數不一致。
        UnicodeDecodeError: 檔案不是 UTF-8 編碼。

    Notes:
        按名稱對應欄位的理由參考 ADR-0010。
    """
    df = pd.read_csv(csvfile_path, encoding="utf-8", skipfooter=2, engine="python")

    missing_columns = [c for c in column_map if c not in df.columns]
    if missing_columns:
        logger.warning(f"{csvfile_path} 缺少欄位 {missing_columns}，這些欄位將為 NaN")

    df = df.rename(columns=column_map)
    df = df.reindex(columns=list(column_map.values()))

    return df.map(lambda x: x.strip() if isinstance(x, str) else x)
