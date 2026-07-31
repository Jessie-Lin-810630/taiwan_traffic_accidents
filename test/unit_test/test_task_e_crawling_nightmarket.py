"""驗證 ADR-0005 的回傳契約，以及 ADR-0006 的 Google API 故障分類。"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.task import e_crawling_nightmarket
from src.task.e_crawling_nightmarket import (
    PermanentPlacesAPIError,
    PlacesAPIError,
    TransientPlacesAPIError,
    find_tw_night_markets_list,
    get_place_details,
    search_place_id,
)

URL = "https://zh.wikipedia.org/night_markets"
HEADERS = {"User-Agent": "pytest"}
REGIONS = {"北部": ["臺北市"]}

PAGE_WITH_ONE_MARKET = """
<html><body>
  <h3>臺北市</h3>
  <table class="wikitable">
    <tr><th>名稱</th><th>地址</th></tr>
    <tr><td>士林夜市</td><td>臺北市士林區大東路</td></tr>
  </table>
</body></html>
"""

PAGE_WITHOUT_MARKETS = """
<html><body>
  <h3>臺北市</h3>
  <table class="wikitable">
    <tr><th>名稱</th><th>地址</th></tr>
    <tr><td>某某公園</td><td>臺北市中正區</td></tr>
  </table>
</body></html>
"""


def _fake_response(text: str = PAGE_WITH_ONE_MARKET):
    response = MagicMock()
    response.text = text
    response.raise_for_status.return_value = None
    return response


def test_連線失敗時拋出而非回傳路徑():
    """核心：finally 曾把這個 raise 吞掉，改回傳一個不存在的檔案路徑。"""
    with patch(
        "src.task.e_crawling_nightmarket.requests.get",
        side_effect=requests.exceptions.ConnectionError("dns failure"),
    ):
        with pytest.raises(requests.exceptions.ConnectionError, match="dns failure"):
            find_tw_night_markets_list(URL, HEADERS, REGIONS)


def test_逾時的例外同樣不被吞掉():
    """四個 except 分支都曾因 finally: return 而失效。"""
    with patch(
        "src.task.e_crawling_nightmarket.requests.get",
        side_effect=requests.exceptions.Timeout("timed out"),
    ):
        with pytest.raises(requests.exceptions.Timeout):
            find_tw_night_markets_list(URL, HEADERS, REGIONS)


def test_非_200_回應會拋出():
    """原實作只在 status_code == 200 時解析，其餘安靜地回傳未寫出的路徑。"""
    response = _fake_response()
    response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

    with patch("src.task.e_crawling_nightmarket.requests.get", return_value=response):
        with pytest.raises(requests.exceptions.HTTPError):
            find_tw_night_markets_list(URL, HEADERS, REGIONS)


def test_解析不到任何夜市時拋出():
    """維基頁面改版導致解析落空，不可產出只有標題列的 CSV。"""
    with patch(
        "src.task.e_crawling_nightmarket.requests.get",
        return_value=_fake_response(PAGE_WITHOUT_MARKETS),
    ):
        with pytest.raises(ValueError, match="解析不到任何夜市"):
            find_tw_night_markets_list(URL, HEADERS, REGIONS)


def test_成功時回傳確實存在的檔案路徑(tmp_path, monkeypatch):
    """回傳值的新契約：這個檔案存在，而且有內容。"""
    monkeypatch.chdir(tmp_path)

    with patch(
        "src.task.e_crawling_nightmarket.requests.get",
        return_value=_fake_response(),
    ):
        csv_path = find_tw_night_markets_list(URL, HEADERS, REGIONS)

    import pandas as pd

    df = pd.read_csv(csv_path)
    assert df["Night_market_name"].tolist() == ["士林夜市"]
    assert df["City"].tolist() == ["臺北市"]
    assert df["Region"].tolist() == ["北部"]


# --- ADR-0006：Google Places API 的 status 三分法 -----------------------------


@pytest.fixture(autouse=True)
def _no_sleep():
    """略過重試退避的實際等待。"""
    with patch("tenacity.nap.time.sleep", return_value=None):
        yield


def _api_response(payload: dict):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_zero_results_回_none_且不重試():
    """真的查無此地點，不是故障 —— 呼叫端會計入失敗清單。"""
    response = _api_response({"status": "ZERO_RESULTS", "candidates": []})

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=response
    ) as fake_get:
        assert search_place_id("不存在的夜市") is None

    assert fake_get.call_count == 1


def test_ok_時回傳_place_id():
    """正常路徑。"""
    response = _api_response({"status": "OK", "candidates": [{"place_id": "ChIJxxx"}]})

    with patch("src.task.e_crawling_nightmarket.requests.get", return_value=response):
        assert search_place_id("士林夜市") == "ChIJxxx"


def test_over_query_limit_會重試後拋出():
    """配額超限是暫時性故障；原實作把它當成「找不到地點」。"""
    response = _api_response({"status": "OVER_QUERY_LIMIT"})

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=response
    ) as fake_get:
        with pytest.raises(TransientPlacesAPIError, match="OVER_QUERY_LIMIT"):
            search_place_id("士林夜市")

    assert fake_get.call_count == e_crawling_nightmarket.RETRY_ATTEMPTS


def test_request_denied_立即拋出不重試():
    """金鑰失效重試無用，只會浪費配額並延後告警。"""
    response = _api_response({"status": "REQUEST_DENIED"})

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=response
    ) as fake_get:
        with pytest.raises(PermanentPlacesAPIError, match="REQUEST_DENIED"):
            search_place_id("士林夜市")

    assert fake_get.call_count == 1


def test_invalid_request_立即拋出不重試():
    """參數錯誤同樣是永久性故障。"""
    response = _api_response({"status": "INVALID_REQUEST"})

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=response
    ) as fake_get:
        with pytest.raises(PermanentPlacesAPIError, match="INVALID_REQUEST"):
            search_place_id("士林夜市")

    assert fake_get.call_count == 1


def test_重試後成功時回傳結果():
    """第一次 OVER_QUERY_LIMIT、第二次 OK。"""
    responses = [
        _api_response({"status": "OVER_QUERY_LIMIT"}),
        _api_response({"status": "OK", "candidates": [{"place_id": "ChIJxxx"}]}),
    ]

    with patch("src.task.e_crawling_nightmarket.requests.get", side_effect=responses):
        assert search_place_id("士林夜市") == "ChIJxxx"


def test_get_place_details_同樣套用三分法():
    """兩支 API 函式的故障分類必須一致。"""
    denied = _api_response({"status": "REQUEST_DENIED"})
    with patch("src.task.e_crawling_nightmarket.requests.get", return_value=denied):
        with pytest.raises(PermanentPlacesAPIError, match="REQUEST_DENIED"):
            get_place_details("ChIJxxx")

    zero = _api_response({"status": "ZERO_RESULTS"})
    with patch("src.task.e_crawling_nightmarket.requests.get", return_value=zero):
        assert get_place_details("ChIJxxx") is None


def test_缺少_status_欄位視為暫時性錯誤():
    """回應格式異常時傾向重試，而非當成「找不到」。"""
    response = _api_response({"candidates": []})

    with patch("src.task.e_crawling_nightmarket.requests.get", return_value=response):
        with pytest.raises(TransientPlacesAPIError):
            search_place_id("士林夜市")


# --- ADR-0006：失敗率門檻 -----------------------------------------------------


def _markets_csv(tmp_path, names: list[str]) -> str:
    import pandas as pd

    path = tmp_path / "markets.csv"
    pd.DataFrame({"Night_market_name": names}).to_csv(path, index=False)
    return str(path)


def test_失敗率超過門檻時拋出(tmp_path, monkeypatch):
    """三個夜市有兩個查不到（67% > 50%），代表系統性問題。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(e_crawling_nightmarket, "API_KEY", "fake-key")
    csv_path = _markets_csv(tmp_path, ["A夜市", "B夜市", "C夜市"])

    with patch.object(
        e_crawling_nightmarket, "search_place_id", side_effect=[None, None, "ChIJxxx"]
    ):
        with patch.object(
            e_crawling_nightmarket, "get_place_details", return_value={"status": "OK"}
        ):
            with pytest.raises(ValueError, match="失敗率"):
                e_crawling_nightmarket.e_crawling_nightmarket(csv_path)


def test_失敗率未超過門檻時正常產出(tmp_path, monkeypatch):
    """四個夜市有一個查不到（25%），屬常態，不該中斷。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(e_crawling_nightmarket, "API_KEY", "fake-key")
    csv_path = _markets_csv(tmp_path, ["A夜市", "B夜市", "C夜市", "D夜市"])

    with patch.object(
        e_crawling_nightmarket,
        "search_place_id",
        side_effect=[None, "id1", "id2", "id3"],
    ):
        with patch.object(
            e_crawling_nightmarket, "get_place_details", return_value={"status": "OK"}
        ):
            json_path = e_crawling_nightmarket.e_crawling_nightmarket(csv_path)

    import json

    with open(json_path, encoding="utf-8") as f:
        assert len(json.load(f)) == 3


def test_全部查不到時拋出(tmp_path, monkeypatch):
    """金鑰失效已由 status 分類攔下；這裡涵蓋的是名稱全面對不上。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(e_crawling_nightmarket, "API_KEY", "fake-key")
    csv_path = _markets_csv(tmp_path, ["A夜市", "B夜市"])

    with patch.object(e_crawling_nightmarket, "search_place_id", return_value=None):
        with pytest.raises(ValueError, match="失敗率"):
            e_crawling_nightmarket.e_crawling_nightmarket(csv_path)


def test_暫時性與永久性例外可分別捕捉():
    """兩者共用基底但互不相容 —— 這是 tenacity 只重試暫時性故障的前提。"""
    assert issubclass(TransientPlacesAPIError, PlacesAPIError)
    assert issubclass(PermanentPlacesAPIError, PlacesAPIError)
    assert not issubclass(TransientPlacesAPIError, PermanentPlacesAPIError)
    assert not issubclass(PermanentPlacesAPIError, TransientPlacesAPIError)


def test_兩種永久性失敗的訊息指向不同的排查方向():
    """REQUEST_DENIED 是授權問題，INVALID_REQUEST 是呼叫端的參數錯誤。"""
    denied = _api_response({"status": "REQUEST_DENIED"})
    with patch("src.task.e_crawling_nightmarket.requests.get", return_value=denied):
        with pytest.raises(PermanentPlacesAPIError, match="金鑰"):
            search_place_id("士林夜市")

    invalid = _api_response({"status": "INVALID_REQUEST"})
    with patch("src.task.e_crawling_nightmarket.requests.get", return_value=invalid):
        with pytest.raises(PermanentPlacesAPIError, match="參數"):
            search_place_id("士林夜市")


def test_not_found_視為查無資料而非成功():
    """Place Details 專有的 NOT_FOUND（place_id 已失效）不可被當成資料回傳。"""
    response = _api_response({"status": "NOT_FOUND"})

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=response
    ) as fake_get:
        assert get_place_details("過期的id") is None

    assert fake_get.call_count == 1


def test_傳輸層的暫時性故障同樣會重試():
    """單一裝飾器要同時涵蓋兩種來源：body 層的 status 與傳輸層的 5xx。"""
    error_response = MagicMock()
    error_response.status_code = 503
    error_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "503", response=error_response
    )

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=error_response
    ) as fake_get:
        with pytest.raises(requests.exceptions.HTTPError):
            search_place_id("士林夜市")

    assert fake_get.call_count == e_crawling_nightmarket.RETRY_ATTEMPTS


def test_傳輸層的永久性故障不重試():
    """403 不該重試 —— 與 body 層的 REQUEST_DENIED 一致。"""
    error_response = MagicMock()
    error_response.status_code = 403
    error_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "403", response=error_response
    )

    with patch(
        "src.task.e_crawling_nightmarket.requests.get", return_value=error_response
    ) as fake_get:
        with pytest.raises(requests.exceptions.HTTPError):
            search_place_id("士林夜市")

    assert fake_get.call_count == 1
