"""共用日誌工具，提供不依賴執行環境的 logging 記錄器。

本模組刻意只依賴 Python 標準函式庫，確保在 Airflow 容器、Cloud Run 的 Streamlit 容器、
pytest 、unittest 與地端腳本中都能無條件被匯入。
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """取得以呼叫端模組命名的 logging 記錄器。

    只有在 root logger 尚未設定任何 handler 時才會補上預設格式，因此在
    Airflow 容器內（root logger 已由 Airflow 設定）會沿用 Airflow 的設定，
    在地端與 pytest 則使用本函式設定的格式，不需要偵測執行環境。

    Args:
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
