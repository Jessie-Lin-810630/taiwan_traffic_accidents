"""驗證 data.gov.tw 專屬的解析與篩選邏輯（ADR-0006）。

通用的抓取行為（重試、故障分類）在 `test_util_crawling_utils.py`；
本檔只涵蓋「因資料來源而異」的部分。
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.task.e_crawling_traffic_accident import (
    _is_accident_csv,
    find_download_links,
)
from src.util import crawling_utils

URL_2021 = "https://data.gov.tw/dataset/158865"
URL_2022 = "https://data.gov.tw/dataset/161199"
HEADERS = {"User-Agent": "pytest"}

PAGE_WITH_LINK = """
<html><body>
  <a title="下載檔案 ZIP" href="https://data.gov.tw/file/a.zip">下載</a>
</body></html>
"""


@pytest.fixture(autouse=True)
def _no_sleep():
    """略過重試退避的實際等待。"""
    with patch("tenacity.nap.time.sleep", return_value=None):
        yield


def _ok_response(text: str = PAGE_WITH_LINK):
    response = MagicMock()
    response.text = text
    response.status_code = 200
    response.raise_for_status.return_value = None
    return response


def _error_response(status_code: int):
    response = _ok_response()
    response.status_code = status_code
    response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        f"{status_code}", response=response
    )
    return response


# --- find_download_links：data.gov.tw 專屬的解析 ------------------------------


def test_解析出下載連結與檔案類型():
    """回傳結構是 {下載連結: (檔案類型, 頁面標題)}。"""
    with patch("src.util.crawling_utils.requests.get", return_value=_ok_response()):
        links = find_download_links([URL_2021], HEADERS)

    assert links == {"https://data.gov.tw/file/a.zip": ("ZIP", None)}


def test_任一頁面失敗即中止而非靜默跳過():
    """原實作非 200 時整個 url 被跳過，回傳的 dict 少一年份卻無人知曉。"""
    with patch(
        "src.util.crawling_utils.requests.get",
        side_effect=[_ok_response(), _error_response(404)],
    ):
        with pytest.raises(requests.exceptions.HTTPError):
            find_download_links([URL_2021, URL_2022], HEADERS)


def test_重試單位是單一請求而非整個清單():
    """第二個頁面 503 時，第一個已成功的頁面不該重抓。"""
    failing = _error_response(503)

    with patch(
        "src.util.crawling_utils.requests.get",
        side_effect=[_ok_response(), failing, failing, failing],
    ) as fake_get:
        with pytest.raises(requests.exceptions.HTTPError):
            find_download_links([URL_2021, URL_2022], HEADERS)

    # 1 次（第一頁成功）+ 3 次（第二頁重試耗盡）；若重試整個迴圈會遠多於 4
    assert fake_get.call_count == 1 + crawling_utils.RETRY_ATTEMPTS


# --- _is_accident_csv：交通事故資料專屬的篩選 --------------------------------


@pytest.mark.parametrize(
    "filename",
    ["110年A1.csv", "NPA_TMA1.csv", "台北市A2交通事故.csv"],
)
def test_取出_a1_與_a2_的_csv(filename):
    """A1（死亡）與 A2（受傷）是本專案要的兩類事故資料。"""
    assert _is_accident_csv(filename) is True


@pytest.mark.parametrize(
    "filename",
    ["readme.txt", "A1說明.pdf", "其他統計.csv", "A1.xlsx"],
)
def test_排除非_a1a2_的檔案(filename):
    """壓縮檔內常夾帶說明文件與其他統計，不可一併寫進 processed_data。"""
    assert _is_accident_csv(filename) is False


# --- SSL：本模組是全專案唯一停用憑證驗證之處 ---------------------------------


def test_data_gov_tw_的請求停用憑證驗證():
    """data.gov.tw 的憑證鏈有問題，由本模組明確傳入 verify=False。"""
    with patch(
        "src.util.crawling_utils.requests.get", return_value=_ok_response()
    ) as fake_get:
        find_download_links([URL_2021], HEADERS)

    assert fake_get.call_args.kwargs["verify"] is False
