"""驗證 ADR-0005：夜市清單抓取的回傳值代表檔案真的產出。"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.task.e_crawling_nightmarket import find_tw_night_markets_list

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
