"""Unit tests for src.task.t_fact_night_markets module."""

import json
from pathlib import Path

import pandas as pd

from src.task.t_fact_night_markets import (
    clean_business_datetime,
    clean_night_market_address,
    clean_night_market_geometry_location,
    clean_night_market_name,
    generate_night_market_serial_num_list,
    read_googlemap_responsed_json,
    t_clean_one_night_market,
    t_fact_night_markets,
)
from src.task.e_crawling_nightmarket import cities_per_region


class TestTFactNightMarketsHelpers:
    """Test suite for helper functions in t_fact_night_markets."""

    def test_generate_night_market_serial_num_list(self, tmp_path):
        """It should return a numeric index for every json record."""
        json_path = tmp_path / "nightmarkets.json"
        json_path.write_text(json.dumps([{"result": {}}, {"result": {}}, {"result": {}}], ensure_ascii=False), encoding="utf-8")

        assert generate_night_market_serial_num_list(json_path) == [0, 1, 2]

    def test_read_googlemap_responsed_json_filters_non_night_markets(self, tmp_path):
        """It should keep only names containing 夜市 or 商圈."""
        json_path = tmp_path / "nightmarkets.json"
        payload = [
            {"result": {"name": "士林夜市"}},
            {"result": {"name": "臺北車站"}},
            {"result": {"name": "西門商圈"}},
        ]
        json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

        result = read_googlemap_responsed_json(json_path)

        assert result == [{"name": "士林夜市"}, {"name": "西門商圈"}]

    def test_clean_night_market_name_removes_nonessential_parenthesis_suffix(self):
        """It should trim parenthesized suffixes that are not part of the name."""
        result = clean_night_market_name({"name": "士林夜市（捷運劍潭站旁）"})

        assert result == {"nightmarket_name": "士林夜市"}

    def test_clean_night_market_address_extracts_region_city_and_zipcode(self):
        """It should normalize address fields from formatted_address."""
        result = clean_night_market_address(
            {"formatted_address": "111台灣台北市士林區文林路101號"},
            cities_per_region,
        )

        assert result["region"] == "北部"
        assert result["city"] == "臺北市"
        assert result["district"] == "士林區"
        assert result["zipcode"] == "111"
        assert result["area_road"] == "111臺灣臺北市士林區文林路101號"

    def test_clean_night_market_geometry_location_returns_numeric_fields(self):
        """It should flatten the location and viewport geometry."""
        result = clean_night_market_geometry_location({
            "geometry": {
                "location": {"lat": 25.1, "lng": 121.5},
                "viewport": {
                    "northeast": {"lat": 25.2, "lng": 121.6},
                    "southwest": {"lat": 25.0, "lng": 121.4},
                },
            }
        })

        assert result == {
            "latitude": 25.1,
            "longitude": 121.5,
            "northeast_latitude": 25.2,
            "northeast_longitude": 121.6,
            "southwest_latitude": 25.0,
            "southwest_longitude": 121.4,
        }

    def test_clean_business_datetime_splits_cross_day_periods(self):
        """It should split overnight business hours into two weekday rows."""
        result = clean_business_datetime({
            "opening_hours": {
                "periods": [
                    {"open": {"day": 6, "time": "1700"}, "close": {"day": 0, "time": "0100"}}
                ]
            }
        })

        assert result == [
            {
                "business_days_weekday": "星期六",
                "business_hours_opening": "17:00:00",
                "business_hours_closing": "23:59:59",
            },
            {
                "business_days_weekday": "星期日",
                "business_hours_opening": "00:00:00",
                "business_hours_closing": "01:00:00",
            },
        ]


class TestTFactNightMarkets:
    """Test suite for t_clean_one_night_market and t_fact_night_markets."""

    def test_t_clean_one_night_market_returns_records(self):
        """It should expand one night market into one row per business period."""
        night_market = {
            "name": "士林夜市（捷運站旁）",
            "formatted_address": "111台灣台北市士林區文林路101號",
            "rating": 4.2,
            "url": "https://maps.example/shilin",
            "geometry": {
                "location": {"lat": 25.088, "lng": 121.525},
                "viewport": {
                    "northeast": {"lat": 25.089, "lng": 121.526},
                    "southwest": {"lat": 25.087, "lng": 121.524},
                },
            },
            "opening_hours": {
                "periods": [
                    {"open": {"day": 1, "time": "1600"}, "close": {"day": 1, "time": "2359"}}
                ]
            },
        }

        result = t_clean_one_night_market(night_market, cities_per_region)

        assert result == [{
            "business_days_weekday": "星期一",
            "business_hours_opening": "16:00:00",
            "business_hours_closing": "23:59:00",
            "nightmarket_name": "士林夜市",
            "region": "北部",
            "city": "臺北市",
            "district": "士林區",
            "zipcode": "111",
            "area_road": "111臺灣臺北市士林區文林路101號",
            "latitude": 25.088,
            "longitude": 121.525,
            "northeast_latitude": 25.089,
            "northeast_longitude": 121.526,
            "southwest_latitude": 25.087,
            "southwest_longitude": 121.524,
            "url_to_googlemap": "https://maps.example/shilin",
            "googlemap_rating": 4.2,
        }]

    def test_t_fact_night_markets_deduplicates_and_replaces_nan(self):
        """It should merge records, deduplicate them, and convert NaN to None."""
        night_markets = [
            {
                "name": "士林夜市",
                "formatted_address": "111台灣台北市士林區文林路101號",
                "rating": None,
                "url": "https://maps.example/shilin",
                "geometry": {"location": {"lat": 25.088, "lng": 121.525}, "viewport": {}},
                "opening_hours": {
                    "periods": [
                        {"open": {"day": 1, "time": "1600"}, "close": {"day": 1, "time": "2200"}}
                    ]
                },
            },
            {
                "name": "士林夜市",
                "formatted_address": "111台灣台北市士林區文林路101號",
                "rating": None,
                "url": "https://maps.example/shilin",
                "geometry": {"location": {"lat": 25.088, "lng": 121.525}, "viewport": {}},
                "opening_hours": {
                    "periods": [
                        {"open": {"day": 1, "time": "1600"}, "close": {"day": 1, "time": "2200"}}
                    ]
                },
            },
        ]

        result = t_fact_night_markets(night_markets, cities_per_region)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["googlemap_rating"] is None
