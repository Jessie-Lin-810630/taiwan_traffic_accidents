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


class TestGetAccidentTableWithMainDay:
    """事故主檔加日期維度：日期必填、走 bind parameter、tooltip 預先組好。"""

    @pytest.mark.parametrize(
        "start_date, end_date",
        [
            (None, (2024, 12, 31)),
            ((2024, 1, 1), None),
            (None, None),
            ((), (2024, 12, 31)),
        ],
        ids=["缺起日", "缺迄日", "都沒給", "空序列"],
    )
    def test_日期沒有成對傳入就拋出(self, start_date, end_date):
        """日期不成對時拋出，不會靜默放寬成撈全表（ADR-0016）。"""
        with patch.object(db, "get_table_from_sqlserver") as m:
            with pytest.raises(ValueError, match="成對傳入"):
                db.get_accident_table_with_main_day(start_date, end_date)

        m.assert_not_called()

    @pytest.mark.parametrize(
        "start_date, end_date",
        [
            ((2024, 1), (2024, 12, 31)),
            ((2024, 1, 1), (2024, 12, 31, 23)),
            (("2024", 1, 1), (2024, 12, 31)),
            ((2024.0, 1, 1), (2024, 12, 31)),
        ],
        ids=["起日只有兩個元素", "迄日有四個元素", "年份是字串", "年份是浮點數"],
    )
    def test_日期不是三個整數就拋出(self, start_date, end_date):
        """格式檢查在發查詢之前，錯的日期不會被送進 SQL。"""
        with patch.object(db, "get_table_from_sqlserver") as m:
            with pytest.raises(ValueError, match="三個整數"):
                db.get_accident_table_with_main_day(start_date, end_date)

        m.assert_not_called()

    def test_日期以_bind_parameter_傳入而非內插進查詢字串(self):
        """日期的數字不得出現在查詢字串裡（ADR-0009 的三條不可退讓之一）。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            db.get_accident_table_with_main_day((2024, 1, 1), (2024, 12, 31))

        sql, params = m.call_args.args[0], m.call_args.args[1]
        assert ":start_date" in sql and ":end_date" in sql
        assert "2024" not in sql
        assert params == {"start_date": "2024-1-1", "end_date": "2024-12-31"}

    def test_查詢串接日期維度表(self):
        """事故日期在維度表上，主檔只有 day_id，因此必須 JOIN。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            db.get_accident_table_with_main_day((2024, 1, 1), (2024, 12, 31))

        sql = m.call_args.args[0]
        assert "fact_accident_main" in sql
        assert "JOIN dim_accident_day" in sql
        assert "BETWEEN" in sql

    def test_查詢結果非空時預先組好_tooltip_文字(self):
        """Tooltip 在查詢階段組好，前端地圖渲染迴圈就不必逐點算。"""
        df = pd.DataFrame(
            {
                "accident_date": ["2024-01-01"],
                "accident_time": ["08:15:00"],
                "death_count": [0],
                "injury_count": [1],
            }
        )
        with patch.object(db, "get_table_from_sqlserver", return_value=df):
            result = db.get_accident_table_with_main_day((2024, 1, 1), (2024, 12, 31))

        assert result["tooltip_text"].iloc[0] == (
            "事故日期時間：2024-01-01 08:15:00\n死亡：0 人\n受傷：1 人"
        )

    def test_tooltip_的死傷人數一律轉成整數(self):
        """死傷是人數，浮點數的「0.0 人」不該出現在畫面上。"""
        df = pd.DataFrame(
            {
                "accident_date": ["2024-01-01"],
                "accident_time": ["08:15:00"],
                "death_count": [0.0],
                "injury_count": [2.0],
            }
        )
        with patch.object(db, "get_table_from_sqlserver", return_value=df):
            result = db.get_accident_table_with_main_day((2024, 1, 1), (2024, 12, 31))

        assert "死亡：0 人" in result["tooltip_text"].iloc[0]
        assert "受傷：2 人" in result["tooltip_text"].iloc[0]

    def test_查詢結果為空時不加_tooltip_欄(self):
        """空表沒有列可以組 tooltip，直接原樣回傳。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()):
            result = db.get_accident_table_with_main_day((2024, 1, 1), (2024, 12, 31))

        assert result.empty
        assert "tooltip_text" not in result.columns

    def test_缺少組_tooltip_所需欄位時靜默跳過(self):
        """欄位不齊時不拋出，只是沒有 tooltip，地圖仍畫得出來。"""
        df = pd.DataFrame({"accident_date": ["2024-01-01"], "death_count": [0]})
        with patch.object(db, "get_table_from_sqlserver", return_value=df):
            result = db.get_accident_table_with_main_day((2024, 1, 1), (2024, 12, 31))

        assert "tooltip_text" not in result.columns


class TestGetAccidentHotspots:
    """事故熱點：範圍過濾與座標聚合都在 SQL 完成（ADR-0016）。"""

    def test_五個參數全部走_bind_parameter(self):
        """座標與門檻的字面值都不得出現在查詢字串裡。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            db.get_accident_hotspots(
                lat_min=21.755,
                lat_max=25.93916,
                lon_min=119.30083,
                lon_max=124.56916,
                min_count=3,
            )

        sql, params = m.call_args.args[0], m.call_args.args[1]
        assert params == {
            "lat_min": 21.755,
            "lat_max": 25.93916,
            "lon_min": 119.30083,
            "lon_max": 124.56916,
            "min_count": 3,
        }
        for literal in ("21.755", "25.93916", "119.30083", "124.56916"):
            assert literal not in sql

    def test_聚合與門檻過濾寫在_sql_而非交給_pandas(self):
        """減量下推是本函式存在的理由，回傳列數要是熱點數而非事故數。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            db.get_accident_hotspots(1.0, 2.0, 3.0, 4.0, 3)

        sql = m.call_args.args[0]
        assert "COUNT(*)" in sql
        assert "GROUP BY latitude, longitude" in sql
        assert "HAVING COUNT(*) >= :min_count" in sql

    def test_不_join_日期維度表也不組_tooltip(self):
        """熱點是聚合後的座標，不對應單一事故，這兩者都用不到。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            db.get_accident_hotspots(1.0, 2.0, 3.0, 4.0, 3)

        sql = m.call_args.args[0]
        assert "JOIN" not in sql
        assert "dim_accident_day" not in sql


class TestFullTableQueries:
    """事故環境與當事人：全表查詢。"""

    @pytest.mark.parametrize(
        "func_name, table_name",
        [
            ("get_accident_table_with_env", "fact_accident_env"),
            ("get_accident_table_with_human", "fact_accident_human"),
        ],
    )
    def test_查詢對應的事實表全表(self, func_name, table_name):
        """兩支都是無參數的全表查詢，只差在查哪一張表。"""
        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            getattr(db, func_name)()

        assert table_name in m.call_args.args[0]
        assert m.call_args.kwargs["database"] == "traffic_accidents"


class TestMartTableQueries:
    """mart 層分析表：未給查詢字串時撈全表，給了就必須把 params 一起往下傳。"""

    @pytest.mark.parametrize(
        "func_name, table_name",
        [
            (
                "get_accident_table_caused_by_pedestrian",
                "analysis_pesdestrian_causing_accident",
            ),
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
            "get_accident_table_caused_by_pedestrian",
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
            "get_accident_table_caused_by_pedestrian",
            "get_accident_table_pedestrian_involved_in",
        ],
    )
    def test_自訂查詢未帶_params_時傳_none(self, func_name):
        """沒有佔位符的查詢照樣要把 params 位置補上，簽章才一致。"""
        dql = "SELECT * FROM t"

        with patch.object(db, "get_table_from_sqlserver", return_value=_empty()) as m:
            getattr(db, func_name)(dql)

        assert m.call_args.args[1] is None
