"""Unit tests for src.task.e_crawling_nightmarket module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from src.task.e_crawling_nightmarket import (
    cities_per_region,
    e_crawling_nightmarket,
    find_tw_night_markets_list,
    get_place_details,
    search_place_id,
)


class TestFindTwNightMarketsList:
    """Test suite for find_tw_night_markets_list function."""

    @patch("src.task.e_crawling_nightmarket.requests.get")
    def test_find_tw_night_markets_list_creates_csv(self, mock_get, monkeypatch, tmp_path):
        """It should parse the wiki tables and persist a csv."""
        monkeypatch.chdir(tmp_path)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html>
          <body>
            <h3>臺北市</h3>
            <table class="wikitable">
              <tr><th>名稱</th><th>地址</th></tr>
              <tr><td>士林夜市</td><td>士林區文林路</td></tr>
              <tr><td>一般市場</td><td>忽略地址</td></tr>
            </table>
          </body>
        </html>
        """
        mock_get.return_value = mock_response

        csv_path = find_tw_night_markets_list("https://example.com", {}, cities_per_region)

        saved_path = Path(csv_path)
        assert saved_path.exists()

        result = pd.read_csv(saved_path)
        assert result.to_dict("records") == [{
            "Unnamed: 0": 0,
            "Region": "北部",
            "City": "臺北市",
            "Night_market_name": "士林夜市",
            "Night_market_address": "士林區文林路",
        }]

    @patch("src.task.e_crawling_nightmarket.requests.get")
    def test_find_tw_night_markets_list_returns_path_on_timeout(self, mock_get, monkeypatch, tmp_path):
        """It should still return the target csv path when request fails."""
        monkeypatch.chdir(tmp_path)
        mock_get.side_effect = requests.exceptions.Timeout()

        csv_path = find_tw_night_markets_list("https://example.com", {}, cities_per_region)

        assert str(Path(csv_path).parent).endswith("test/raw_data")
        assert not Path(csv_path).exists()


class TestGoogleMapHelpers:
    """Test suite for Google Maps API helper functions."""

    @patch("src.task.e_crawling_nightmarket.requests.get")
    def test_search_place_id_returns_first_candidate(self, mock_get):
        """It should return the first candidate place id."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"candidates": [{"place_id": "abc123"}]}
        mock_get.return_value = mock_response

        assert search_place_id("士林夜市") == "abc123"

    @patch("src.task.e_crawling_nightmarket.requests.get")
    def test_search_place_id_returns_none_when_not_found(self, mock_get):
        """It should return None when no candidates are found."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"candidates": []}
        mock_get.return_value = mock_response

        assert search_place_id("未知地點") is None

    @patch("src.task.e_crawling_nightmarket.requests.get")
    def test_get_place_details_returns_json(self, mock_get):
        """It should return the decoded response payload."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": {"name": "士林夜市"}}
        mock_get.return_value = mock_response

        assert get_place_details("abc123") == {"result": {"name": "士林夜市"}}


class TestECrawlingNightmarket:
    """Test suite for e_crawling_nightmarket function."""

    def test_returns_message_when_api_key_missing(self, tmp_path):
        """It should stop early when API key is unavailable."""
        csv_path = tmp_path / "nightmarkets.csv"
        csv_path.write_text("Night_market_name\n士林夜市\n", encoding="utf-8")

        with patch("src.task.e_crawling_nightmarket.API_KEY", None):
            result = e_crawling_nightmarket(csv_path)

        assert result == "找不到 API 金鑰，請確認 .env 檔"

    def test_exports_json_for_successful_rows(self, monkeypatch, tmp_path):
        """It should collect detail payloads and export them to json."""
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "nightmarkets.csv"
        csv_path.write_text("Night_market_name\n士林夜市\n饒河夜市\n", encoding="utf-8")

        detail_payload = {"result": {"name": "士林夜市"}}
        detail_payload_2 = {"result": {"name": "饒河夜市"}}

        with patch("src.task.e_crawling_nightmarket.API_KEY", "fake-key"):
            with patch("src.task.e_crawling_nightmarket.search_place_id", side_effect=["id-1", "id-2"]):
                with patch("src.task.e_crawling_nightmarket.get_place_details", side_effect=[detail_payload, detail_payload_2]):
                    result_path = e_crawling_nightmarket(csv_path)

        saved_path = Path(result_path)
        assert saved_path.exists()
        with saved_path.open("r", encoding="utf-8") as fh:
            assert json.load(fh) == [detail_payload, detail_payload_2]

    def test_skips_rows_when_place_id_or_details_missing(self, monkeypatch, tmp_path):
        """It should persist only successful detail lookups."""
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "nightmarkets.csv"
        csv_path.write_text("Night_market_name\n士林夜市\n饒河夜市\n", encoding="utf-8")

        with patch("src.task.e_crawling_nightmarket.API_KEY", "fake-key"):
            with patch("src.task.e_crawling_nightmarket.search_place_id", side_effect=[None, "id-2"]):
                with patch("src.task.e_crawling_nightmarket.get_place_details", return_value={"result": {"name": "饒河夜市"}}):
                    result_path = e_crawling_nightmarket(csv_path)

        with Path(result_path).open("r", encoding="utf-8") as fh:
            assert json.load(fh) == [{"result": {"name": "饒河夜市"}}]
