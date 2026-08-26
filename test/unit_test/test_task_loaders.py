"""驗證 8 個 loader 對 upsert_to_table 宣告的表名與 update 欄位。

依 ADR-0002，loader 瘦身後只保留宣告，這份宣告即是它們全部的內容，
因此也是唯一值得釘住的東西。
"""

import importlib
from unittest.mock import patch

import pandas as pd
import pytest

# (模組名, 目標表名, 預期的 update_columns)
LOADER_DECLARATIONS = [
    ("l_dim_accident_day", "dim_accident_day", ["accident_weekday"]),
    ("l_dim_accident_type", "dim_accident_type", ["accident_category"]),
    ("l_dim_lane_design", "dim_lane_design", ["lane_edge_marking"]),
    ("l_dim_road_design", "dim_road_design", ["road_form_minor"]),
    ("l_fact_accident_env", "fact_accident_env", ["weather_condition"]),
    ("l_fact_accident_human", "fact_accident_human", ["hit_and_run"]),
    # 唯一鍵四欄與 weather_record_id 都不得列入 update_columns，參考 ADR-0014
    (
        "l_fact_accident_main",
        "fact_accident_main",
        ["accident_type_id", "death_count", "injury_count"],
    ),
    # 唯一鍵三欄（緯度、經度、營業星期）不得列入 update_columns，參考 ADR-0014
    (
        "l_fact_night_markets",
        "fact_night_markets",
        [
            "updated_on",
            "business_hours_closing",
            "business_hours_opening",
            "northeast_latitude",
            "northeast_longitude",
            "southwest_latitude",
            "southwest_longitude",
            "url_to_googlemap",
            "googlemap_rating",
            "nightmarket_name",
        ],
    ),
]


@pytest.mark.parametrize("module_name, table, update_columns", LOADER_DECLARATIONS)
def test_loader_以正確的表名與欄位呼叫_upsert(module_name, table, update_columns):
    """每個 loader 應把自己的表名與 update 欄位轉交給 upsert_to_table。"""
    module = importlib.import_module(f"src.task.{module_name}")
    loader = getattr(module, module_name)
    df = pd.DataFrame({"col_a": [1]})

    with patch(f"src.task.{module_name}.upsert_to_table") as mocked:
        loader(df, "traffic_accidents")

    mocked.assert_called_once()
    _, kwargs = mocked.call_args
    assert kwargs["table"] == table
    assert kwargs["update_columns"] == update_columns
    assert kwargs["database"] == "traffic_accidents"


def test_夜市_loader_寫入前蓋上_updated_on():
    """`fact_night_markets` 獨有的時間戳處理留在該 loader 內（ADR-0002 子決策 4）。"""
    from src.task.l_fact_night_markets import l_fact_night_markets

    df = pd.DataFrame({"nightmarket_name": ["士林夜市"]})

    with patch("src.task.l_fact_night_markets.upsert_to_table") as mocked:
        l_fact_night_markets(df)

    passed_df = mocked.call_args[0][0]
    assert "updated_on" in passed_df.columns
    assert passed_df["updated_on"].notna().all()
