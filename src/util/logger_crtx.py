"""共用日誌工具，提供不依賴執行環境的 logging 記錄器。

本模組刻意只依賴標準函式庫，確保在 Airflow 容器、Cloud Run 的 Streamlit 容器、
pytest 與地端腳本中都能無條件被匯入。
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """取得以呼叫端模組命名的 logging 記錄器。

    Airflow 容器內的 root logger 已由 Airflow 自行設定 handler，
    因此 `basicConfig()` 只會在尚未有任何 handler 的環境（地端、pytest）生效，
    不需要偵測是否身處 Airflow。

    Parameters:
        name (str): 記錄器名稱，呼叫端一律傳入 `__name__`。

    Returns:
        logging.Logger: 以 `name` 命名的記錄器。

    Examples:
        >>> from src.util.logger_crtx import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("test")
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
    return logging.getLogger(name)
