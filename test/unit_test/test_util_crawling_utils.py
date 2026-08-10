"""驗證 ADR-0006：抓取層區分暫時性故障、永久性故障與查無資料。"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.util import crawling_utils
from src.util.crawling_utils import (
    download_and_extract_zip,
    download_csv,
    fetch_soup,
    is_transient,
)

URL = "https://data.gov.tw/dataset/158865"
HEADERS = {"User-Agent": "pytest"}

PAGE_WITH_LINK = """
<html><body>
  <a title="下載檔案 CSV" href="https://data.gov.tw/file/a.csv">下載</a>
</body></html>
"""


@pytest.fixture(autouse=True)
def _no_sleep():
    """略過退避的實際等待，避免測試真的睡滿 2+4 秒。

    patch 的是 tenacity 真正呼叫的 `time.sleep`；改 wait 策略物件無效，
    因為 tenacity 呼叫的是 `wait(retry_state)` 而非實例的 `__call__`。
    """
    with patch("tenacity.nap.time.sleep", return_value=None):
        yield


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    response = MagicMock()
    response.status_code = status_code
    return requests.exceptions.HTTPError(f"{status_code}", response=response)


def _ok_response(text: str = PAGE_WITH_LINK):
    response = MagicMock()
    response.text = text
    response.status_code = 200
    response.raise_for_status.return_value = None
    return response


def _error_response(status_code: int):
    """回傳會在 raise_for_status() 拋出指定狀態碼的假回應。"""
    response = _ok_response()
    response.status_code = status_code
    response.raise_for_status.side_effect = _http_error(status_code)
    return response


# --- 故障分類 ---------------------------------------------------------------


@pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
def test_5xx_與_429_屬於暫時性故障(status_code):
    """這幾種狀態碼重跑可能成功，值得重試。"""
    assert is_transient(_http_error(status_code)) is True


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
def test_其餘_4xx_屬於永久性故障(status_code):
    """網址錯或無權限，重試只會延後告警。"""
    assert is_transient(_http_error(status_code)) is False


def test_連線層錯誤屬於暫時性故障():
    """Timeout 與 ConnectionError 都是典型的暫時性故障。"""
    assert is_transient(requests.exceptions.Timeout()) is True
    assert is_transient(requests.exceptions.ConnectionError()) is True


def test_非_requests_例外不重試():
    """解壓縮或寫檔失敗重試無用，不該被誤判為暫時性。"""
    assert is_transient(ValueError("bad zip")) is False


# --- 重試行為 ---------------------------------------------------------------


def test_暫時性故障會重試指定次數後才拋出():
    """核心：503 應重試 RETRY_ATTEMPTS 次，而非第一次就失敗。"""
    response = _ok_response()
    response.raise_for_status.side_effect = _http_error(503)

    with patch(
        "src.util.crawling_utils.requests.get", return_value=response
    ) as fake_get:
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_soup(URL, HEADERS)

    assert fake_get.call_count == crawling_utils.RETRY_ATTEMPTS


def test_永久性故障不重試():
    """404 只該送出一次請求 —— 重試等於浪費時間並延後告警。"""
    response = _ok_response()
    response.raise_for_status.side_effect = _http_error(404)

    with patch(
        "src.util.crawling_utils.requests.get", return_value=response
    ) as fake_get:
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_soup(URL, HEADERS)

    assert fake_get.call_count == 1


def test_重試後成功不影響回傳值():
    """第一次 503、第二次成功時，呼叫端應拿到正常結果。"""
    failing, ok = _ok_response(), _ok_response()
    failing.raise_for_status.side_effect = _http_error(503)

    with patch("src.util.crawling_utils.requests.get", side_effect=[failing, ok]):
        soup = fetch_soup(URL, HEADERS)

    assert soup.find("a").get("href") == "https://data.gov.tw/file/a.csv"


def test_重試耗盡時拋出原始例外而非_retryerror():
    """reraise=True 讓呼叫端看到的是 HTTPError，而不是 tenacity 的包裝。"""
    response = _ok_response()
    response.raise_for_status.side_effect = _http_error(503)

    with patch("src.util.crawling_utils.requests.get", return_value=response):
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_soup(URL, HEADERS)


# --- download_csv 的 headers ------------------------------------------------


def test_download_csv_會帶上傳入的_headers(tmp_path):
    """原實作硬編碼 headers={}，data.gov.tw 可能拒絕沒有 User-Agent 的請求。"""
    response = _ok_response()
    response.iter_content.return_value = [b"col\n1\n"]

    with patch(
        "src.util.crawling_utils.requests.get", return_value=response
    ) as fake_get:
        paths = download_csv(
            "https://data.gov.tw/file/a.csv", HEADERS, "a.csv", tmp_path
        )

    assert fake_get.call_args.kwargs["headers"] == HEADERS
    assert len(paths) == 1


# --- download_and_extract_zip 的檔名篩選 -------------------------------------


def _zip_with(tmp_path, names: list[str]):
    """建一個含指定檔名的 zip，並回傳假的下載回應。"""
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        for name in names:
            z.writestr(name, "col\n1\n")
    payload = buffer.getvalue()

    response = _ok_response()
    response.iter_content.return_value = [payload]
    return response


def test_預設取出所有_csv(tmp_path):
    """Util 不預設任何資料來源的命名規則。"""
    response = _zip_with(tmp_path, ["a.csv", "b.csv", "readme.txt"])

    with patch("src.util.crawling_utils.requests.get", return_value=response):
        paths = download_and_extract_zip(
            "https://example.com/a.zip", HEADERS, tmp_path, "a.zip", tmp_path
        )

    assert sorted(p.split("/")[-1] for p in paths) == ["a.csv", "b.csv"]


def test_篩選條件由呼叫端決定(tmp_path):
    """各資料來源的命名規則不同，篩選規則不該寫死在 util。"""
    response = _zip_with(tmp_path, ["A1.csv", "A2.csv", "其他.csv"])

    with patch("src.util.crawling_utils.requests.get", return_value=response):
        paths = download_and_extract_zip(
            "https://example.com/a.zip",
            HEADERS,
            tmp_path,
            "a.zip",
            tmp_path,
            filename_filter=lambda name: name.startswith("A1"),
        )

    assert [p.split("/")[-1] for p in paths] == ["A1.csv"]


# --- 日誌級別：會被重試的不該用 ERROR ----------------------------------------


def test_暫時性故障只記_warning(caplog):
    """一次最終失敗不該在監控上放大成三筆 ERROR 告警（ADR-0006）。"""
    with patch(
        "src.util.crawling_utils.requests.get", return_value=_error_response(503)
    ):
        with pytest.raises(requests.exceptions.HTTPError):
            with caplog.at_level("WARNING", logger="src.util.crawling_utils"):
                fetch_soup(URL, HEADERS)

    assert [r.levelname for r in caplog.records].count("ERROR") == 0
    assert any("暫時性故障" in r.message for r in caplog.records)


def test_永久性故障記_error(caplog):
    """404 不會重試，那一筆就是確定的失敗。"""
    with patch(
        "src.util.crawling_utils.requests.get", return_value=_error_response(404)
    ):
        with pytest.raises(requests.exceptions.HTTPError):
            with caplog.at_level("WARNING", logger="src.util.crawling_utils"):
                fetch_soup(URL, HEADERS)

    assert [r.levelname for r in caplog.records].count("ERROR") == 1


# --- SSL 憑證驗證：預設安全，停用需明確傳入 ----------------------------------


def test_預設驗證_ssl_憑證():
    """Util 不替呼叫端決定停用憑證驗證（ADR-0006 子決策 6）。"""
    with patch(
        "src.util.crawling_utils.requests.get", return_value=_ok_response()
    ) as fake_get:
        fetch_soup(URL, HEADERS)

    assert fake_get.call_args.kwargs["verify"] is True


def test_呼叫端可明確停用驗證():
    """憑證鏈有問題的來源由呼叫端自行承擔並記錄。"""
    with patch(
        "src.util.crawling_utils.requests.get", return_value=_ok_response()
    ) as fake_get:
        fetch_soup(URL, HEADERS, verify=False)

    assert fake_get.call_args.kwargs["verify"] is False


def test_兩支下載函式同樣預設驗證(tmp_path):
    """三支對外函式的預設值必須一致，否則會有安全破口。"""
    response = _ok_response()
    response.iter_content.return_value = [b"col\n1\n"]

    with patch(
        "src.util.crawling_utils.requests.get", return_value=response
    ) as fake_get:
        download_csv("https://example.com/a.csv", HEADERS, "a.csv", tmp_path)
    assert fake_get.call_args.kwargs["verify"] is True

    zip_response = _zip_with(tmp_path, ["a.csv"])
    with patch(
        "src.util.crawling_utils.requests.get", return_value=zip_response
    ) as fake_get:
        download_and_extract_zip(
            "https://example.com/a.zip", HEADERS, tmp_path, "a.zip", tmp_path
        )
    assert fake_get.call_args.kwargs["verify"] is True
