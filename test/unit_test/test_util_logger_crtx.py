"""驗證 logger 工具的行為，以及 util 連線模組在無 Airflow 的環境可被匯入。"""

import importlib
import logging
from pathlib import Path

from src.util.logger_crtx import get_logger


def test_get_logger_以傳入名稱命名記錄器():
    """Logger 名稱應為呼叫端傳入的 __name__，而非 logger_crtx 自身。"""
    logger = get_logger("src.util.some_module")

    assert isinstance(logger, logging.Logger)
    assert logger.name == "src.util.some_module"


def test_get_logger_不覆寫既有的_root_handler():
    """Root logger 已有 handler 時（如 Airflow 容器內）不應再呼叫 basicConfig。"""
    root = logging.getLogger()
    sentinel = logging.NullHandler()
    root.addHandler(sentinel)
    try:
        before = list(root.handlers)
        get_logger(__name__)

        assert root.handlers == before
    finally:
        root.removeHandler(sentinel)


def test_util連線模組在無_airflow_的環境可匯入():
    """三個 util 模組不得於匯入期依賴 airflow，否則 Cloud Run 與 pytest 會失敗。"""
    for module_name in (
        "src.util.mysql_utils",
        "src.util.redis_utils",
        "src.util.crawling_utils",
    ):
        assert importlib.import_module(module_name) is not None


def test_util連線模組不再持有_airflow_例外類別():
    """例外一律原樣往上拋，模組不應再匯出 AirflowException 或環境旗標。"""
    for module_name in (
        "src.util.mysql_utils",
        "src.util.redis_utils",
        "src.util.crawling_utils",
    ):
        module = importlib.import_module(module_name)

        assert not hasattr(module, "AirflowException")
        assert not hasattr(module, "is_airflow_env")


def _task_module_names() -> list[str]:
    """列出 src/task/ 下所有正式 ETL 模組（排除 temp_try_* 暫存檔）。"""
    task_dir = Path(__file__).resolve().parents[2] / "src" / "task"
    return sorted(
        f"src.task.{path.stem}"
        for path in task_dir.glob("*.py")
        if not path.stem.startswith(("temp_try", "__"))
    )


def test_所有_etl_模組在無_airflow_的環境可匯入():
    """ETL 模組不得於匯入期依賴 airflow，否則地端與 pytest 無法載入。"""
    module_names = _task_module_names()

    assert len(module_names) == 20
    for module_name in module_names:
        assert importlib.import_module(module_name) is not None


def test_etl_模組不再持有_airflow_例外類別():
    """ETL 模組應改為原樣拋出例外，不再匯入 AirflowException 或 Variable。"""
    for module_name in _task_module_names():
        module = importlib.import_module(module_name)

        assert not hasattr(module, "AirflowException")
        assert not hasattr(module, "Variable")
