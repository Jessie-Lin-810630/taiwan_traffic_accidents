"""驗證前端查詢層的查詢契約：參數走 bind、日期驗證、tooltip 組法。

本層只發查詢、不做業務運算，因此測的是「送出去的 SQL 與參數長什麼樣」，
而不是查詢結果。三件被釘住的事：

- 座標與日期一律走 bind parameter，不內插進查詢字串（ADR-0009、ADR-0016）
- 自訂查詢的 `params` 必須往下傳，否則帶佔位符的查詢會缺綁定值
- 熱點查詢的減量在 SQL 完成（ADR-0016）
"""

from unittest.mock import patch

import pandas as pd
import pytest

import src.task.core.c_db as db


def _empty() -> pd.DataFrame:
    """回傳空 DataFrame，給不在意查詢結果的測試當作假回傳值。"""
    return pd.DataFrame()


class TestGetNightMarketsTable:
    """夜市事實表：全表查詢。"""

    def test_查詢夜市事實表全表(self):
        """查的是 fact_night_markets，且指定業務資料庫。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            db.get_night_markets_table()

        sql = m.call_args.args[0]
        assert "fact_night_markets" in sql
        assert m.call_args.kwargs["database"] == "traffic_accidents"


class TestMartTableQueries:
    """mart 層分析表：未給查詢字串時撈全表，給了就必須把 params 一起往下傳。"""

    @pytest.mark.parametrize(
        "func_name, table_name",
        [
            (
                "get_accident_table_pedestrian_involved_in",
                "analysis_pesdestrian_involving_accident",
            ),
        ],
    )
    def test_未給查詢字串時撈全表(self, func_name, table_name):
        """預設路徑查的是各自對應的分析表。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            getattr(db, func_name)()

        assert table_name in m.call_args.args[0]

    @pytest.mark.parametrize(
        "func_name",
        [
            "get_accident_table_pedestrian_involved_in",
        ],
    )
    def test_自訂查詢的_params_必須往下傳(self, func_name):
        """漏傳 params 的話，帶 :min_lat 這類佔位符的查詢會缺綁定值而失敗。"""
        dql = "SELECT * FROM t WHERE latitude BETWEEN :min_lat AND :max_lat"
        params = {"min_lat": 25.05, "max_lat": 25.12}

        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            getattr(db, func_name)(dql, params)

        assert m.call_args.args[0] == dql
        assert m.call_args.args[1] == params

    @pytest.mark.parametrize(
        "func_name",
        [
            "get_accident_table_pedestrian_involved_in",
        ],
    )
    def test_自訂查詢未帶_params_時傳_none(self, func_name):
        """沒有佔位符的查詢照樣要把 params 位置補上，簽章才一致。"""
        dql = "SELECT * FROM t"

        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            getattr(db, func_name)(dql)

        assert m.call_args.args[1] is None
