"""雙環境日誌工具，自動辨識 Airflow 與地端並提供對應的 logger 與例外類別。"""

import importlib.util
import logging
import os
import sys
from logging import Logger
from typing import (
    NamedTuple,  # 用 collections.namedtuple 的話，IDE 只知道定義的參數型別是 Any，沒有提示。
)

from loguru import logger

# 統一對外 export：讓各 ETL 只需 import 這三個物件


class LoggerContext(NamedTuple):
    """`create_logging_logger()` 的回傳值，打包 logger 與環境相關資訊。"""

    logger: Logger  # 比起使用 collections.namedtuple ，這裡可以定義好型別。
    is_airflow_env: bool
    AirflowException: type


def create_logging_logger() -> LoggerContext:
    """自動判斷是否在 Airflow 環境後，設定 logging 記錄器。

    並一併回傳環境旗標與對應的 AirflowException 類別。

    Returns:
        _LoggerContext: NamedTuple，包含 logger、is_airflow_env、AirflowException

    Examples:
        >>> ctx = create_logging_logger()
        >>> ctx.logger.info("test")
        >>> if ctx.is_airflow_env:
        ...     raise ctx.AirflowException("task failed")
    """
    is_airflow_env = any(
        [
            importlib.util.find_spec("airflow"),
            os.getenv("AIRFLOW_HOME"),
            True if "opt/airflow" in sys.path else False,
        ]
    )

    if is_airflow_env:
        from airflow.exceptions import AirflowException

        logger = logging.getLogger("airflow.task")
    else:

        class AirflowException(Exception):
            """地端測試時的虛擬自訂例外，繼承 Exception 類別。

            以防未來程式碼異動或測試情境下，
            `AirflowException` 在 `if is_airflow_env` 保護範圍外被引用時出現 NameError。
            """

            pass

        # 只在尚未設定 handler 時才呼叫，避免多份 ETL 腳本因需要呼叫此函式而重複設定
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
            )
        logger = logging.getLogger(__name__)

    return LoggerContext(
        logger=logger, is_airflow_env=is_airflow_env, AirflowException=AirflowException
    )


def set_loguru_logger() -> logger:
    """回傳 loguru.logger 物件，即可使用 loguru 模組的 logger 紀錄日誌。

    比使用原生的 logging 較美觀與便於設定，
    但如果使用 AirFlow ，建議使用原生 logging ，
    避免部分 log 層級或 format 不相容而無法正常寫入。

    :return: 回傳 loguru.logger 物件
    :rtype: logger

    Examples:
        from src.util.logger_crtx import set_loguru_logger
        logger = set_loguru_logger()
        logger.info("test_logging")
        >>output: 2026-05-31 18:03:16 | INFO     | test_logging

    """
    # 清除 loguru 預設的設定，並重新配置你想要的 Log 等級（例如：INFO）
    logger.remove()

    # 印出在終端機或是輸出日誌檔 .log，以下假設是印出在終端機
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    )
    return logger  # 其他
