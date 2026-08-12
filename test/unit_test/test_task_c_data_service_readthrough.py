"""驗證前端直接呼叫的讀取型函式：read-through 快取、資料清洗與座標綁定。

釘住三件事：

- 快取命中就不查 MySQL，沒命中才查並補寫（ADR-0003 的 read-through 語意）
- 查回來的清洗規則（離島歸屬、座標轉數值、剔除座標缺漏）
- 座標一律走 bind parameter，不內插進查詢字串（ADR-0009）
"""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import src.task.core.c_data_service as ds


def _night_market_row(**overrides) -> dict:
    """一列夜市主檔的預設值，測試只覆寫關心的欄位。"""
    row = {
        "nightmarket_name": "士林夜市",
        "region": "北部",
        "city": "臺北市",
        "district": "士林區",
        "area_road": "基河路 101 號",
        "latitude": 25.0878,
        "longitude": 121.5240,
        "northeast_latitude": 25.09,
        "northeast_longitude": 121.53,
        "southwest_latitude": 25.08,
        "southwest_longitude": 121.52,
        "googlemap_rating": 4.2,
    }
    row.update(overrides)
    return row


class TestGetAllNightmarketsCache:
    """夜市主檔的快取命中條件。"""

    def test_快取命中且欄位齊全時不查_mysql(self):
        """快取有 area_road 與 region 就直接用，不再多跑一趟 MySQL。"""
        cached = [_night_market_row()]
        with patch.object(ds, "get_cache", return_value=cached):
            with patch.object(ds, "get_night_markets_table") as m_db:
                result = ds.get_all_nightmarkets()

        m_db.assert_not_called()
        assert len(result) == 1

    @pytest.mark.parametrize(
        "missing_col", ["area_road", "region"], ids=["缺area_road", "缺region"]
    )
    def test_快取缺關鍵欄位時視為未命中改查_mysql(self, missing_col):
        """舊版快取沒有這兩欄，直接拿來用會讓下拉選單少一層分類。"""
        stale = _night_market_row()
        del stale[missing_col]

        with patch.object(ds, "get_cache", return_value=[stale]):
            with patch.object(
                ds,
                "get_night_markets_table",
                return_value=pd.DataFrame([_night_market_row()]),
            ) as m_db:
                with patch.object(ds, "set_cache"):
                    ds.get_all_nightmarkets()

        m_db.assert_called_once()


class TestGetAllNightmarketsCleaning:
    """夜市主檔的清洗規則。"""

    @pytest.mark.parametrize(
        "field, value",
        [
            ("area_road", "琉球鄉中山路"),
            ("area_road", "蘭嶼鄉紅頭村"),
            ("area_road", "綠島鄉南寮村"),
            ("nightmarket_name", "小琉球夜市"),
            ("nightmarket_name", "蘭嶼夜市"),
            ("nightmarket_name", "綠島夜市"),
        ],
    )
    def test_離島夜市一律歸到東部與東部離島(self, field, value):
        """小琉球、蘭嶼、綠島在原始資料歸屬各異，統一改寫成同一個地區。"""
        row = _night_market_row(region="南部", **{field: value})

        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_night_markets_table", return_value=pd.DataFrame([row])
            ):
                with patch.object(ds, "set_cache"):
                    result = ds.get_all_nightmarkets()

        assert result["region"].iloc[0] == "東部與東部離島"

    def test_非離島夜市的地區歸屬不被改動(self):
        """改寫只針對三個離島關鍵字，其餘夜市維持原本的地區。"""
        row = _night_market_row(region="北部")

        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_night_markets_table", return_value=pd.DataFrame([row])
            ):
                with patch.object(ds, "set_cache"):
                    result = ds.get_all_nightmarkets()

        assert result["region"].iloc[0] == "北部"

    def test_座標缺漏的列被剔除(self):
        """沒有座標的夜市無法算周邊事故，留著只會讓下游 KeyError。"""
        rows = [
            _night_market_row(nightmarket_name="有座標"),
            _night_market_row(nightmarket_name="缺緯度", latitude=None),
            _night_market_row(nightmarket_name="缺經度", longitude=None),
        ]

        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_night_markets_table", return_value=pd.DataFrame(rows)
            ):
                with patch.object(ds, "set_cache"):
                    result = ds.get_all_nightmarkets()

        assert list(result["nightmarket_name"]) == ["有座標"]

    def test_非數值的座標被轉成缺漏後剔除(self):
        """座標欄可能混入文字，先轉數值再剔除，不讓髒資料進到距離計算。"""
        rows = [
            _night_market_row(nightmarket_name="正常"),
            _night_market_row(nightmarket_name="髒資料", latitude="無資料"),
        ]

        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_night_markets_table", return_value=pd.DataFrame(rows)
            ):
                with patch.object(ds, "set_cache"):
                    result = ds.get_all_nightmarkets()

        assert list(result["nightmarket_name"]) == ["正常"]

    def test_補寫快取的存活時間與模組常數一致(self):
        """所有快取共用同一個 TTL，不讓各處各寫一個數字。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds,
                "get_night_markets_table",
                return_value=pd.DataFrame([_night_market_row()]),
            ):
                with patch.object(ds, "set_cache") as m_set:
                    ds.get_all_nightmarkets()

        assert m_set.call_args.args[0] == "market:list_all_auto_v3"
        assert m_set.call_args.kwargs["ttl"] == ds.CACHE_TTL_SECONDS

    def test_呼叫端可覆寫存活時間(self):
        """維護者要臨時縮短某一支的 TTL 時，不必去改模組常數。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds,
                "get_night_markets_table",
                return_value=pd.DataFrame([_night_market_row()]),
            ):
                with patch.object(ds, "set_cache") as m_set:
                    ds.get_all_nightmarkets(cache_ttl=518400)

        assert m_set.call_args.kwargs["ttl"] == 518400


class TestGetNightmarketsForPageSelector:
    """下拉選單用的別名欄位：只改名，不另外讀 MySQL。"""

    def test_補上別名欄位且不另開快取(self):
        """只做欄位改名，主檔的讀取與清洗一律由 get_all_nightmarkets 負責。"""
        with patch.object(
            ds, "get_all_nightmarkets", return_value=pd.DataFrame([_night_market_row()])
        ):
            with patch.object(ds, "get_cache") as m_cache:
                result = ds.get_nightmarkets_for_page_selector()

        m_cache.assert_not_called()
        assert result["MarketName"].iloc[0] == "士林夜市"
        assert result["Region"].iloc[0] == "北部"
        assert result["City"].iloc[0] == "臺北市"
        assert result["lat"].iloc[0] == 25.0878
        assert result["lon"].iloc[0] == 121.5240

    def test_行政區取_district_而非街道地址(self):
        """area_road 是門牌地址，拿它當行政區會讓選單出現一堆路名。"""
        row = _night_market_row(district="士林區", area_road="基河路 101 號")

        with patch.object(ds, "get_all_nightmarkets", return_value=pd.DataFrame([row])):
            result = ds.get_nightmarkets_for_page_selector()

        assert result["AdminDistrict"].iloc[0] == "士林區"

    def test_主檔為空時原樣回傳空表(self):
        """空表不該在改名階段炸掉，前端自行顯示查無資料。"""
        with patch.object(ds, "get_all_nightmarkets", return_value=pd.DataFrame()):
            result = ds.get_nightmarkets_for_page_selector()

        assert result.empty

    def test_不改動主檔本身(self):
        """回傳的是副本，否則會污染 get_all_nightmarkets 的快取內容。"""
        df = pd.DataFrame([_night_market_row()])

        with patch.object(ds, "get_all_nightmarkets", return_value=df):
            ds.get_nightmarkets_for_page_selector()

        assert "MarketName" not in df.columns


class TestHaversineDistance:
    """半正矢距離：純函式，可用已知距離驗證。"""

    def test_同一點的距離是零(self):
        """半正矢公式在兩點重合時必須收斂到 0，不是浮點誤差。"""
        assert ds.haversine_distance(25.0, 121.0, 25.0, 121.0) == 0.0

    def test_赤道上一度經度約_111_公里(self):
        """以已知的地理常數驗證公式，不只是驗證它跑得動。"""
        d = ds.haversine_distance(0.0, 0.0, 0.0, 1.0)
        assert d == pytest.approx(111.19, abs=0.1)

    def test_一度緯度約_111_公里(self):
        """緯度方向的一度距離與經度在赤道上相同。"""
        d = ds.haversine_distance(25.0, 121.0, 26.0, 121.0)
        assert d == pytest.approx(111.19, abs=0.1)

    def test_距離不因兩點順序而改變(self):
        """距離是對稱的，交換起訖點的結果必須相同。"""
        a = ds.haversine_distance(25.0878, 121.5240, 22.6320, 120.3020)
        b = ds.haversine_distance(22.6320, 120.3020, 25.0878, 121.5240)
        assert a == pytest.approx(b)

    def test_可傳入陣列做向量化計算(self):
        """前端逐列算距離會很慢，這支必須支援 Series。"""
        lats = pd.Series([25.0, 26.0])
        result = ds.haversine_distance(lats, 121.0, 25.0, 121.0)

        assert isinstance(result, (pd.Series, np.ndarray))
        assert len(result) == 2
        assert result[0] == pytest.approx(0.0)


class TestGetAccidentHeatmapData:
    """熱點圖：減量已在 SQL 完成，pandas 只負責抽樣（ADR-0016）。"""

    def _hotspots(self, n: int) -> pd.DataFrame:
        """合成 n 個熱點，座標各異以免被聚合。"""
        return pd.DataFrame(
            {
                "latitude": [25.0 + i / 10000 for i in range(n)],
                "longitude": [121.0 + i / 10000 for i in range(n)],
                "count": [3 + i % 5 for i in range(n)],
            }
        )

    def test_快取命中時不查_mysql(self):
        """快取命中就不查 MySQL，這是 read-through 的意義所在。"""
        cached = self._hotspots(2).to_dict("records")
        with patch.object(ds, "get_cache", return_value=cached):
            with patch.object(ds, "get_accident_hotspots") as m_db:
                result = ds.get_accident_heatmap_data()

        m_db.assert_not_called()
        assert len(result) == 2

    def test_查詢帶入臺灣國土範圍與熱點門檻(self):
        """國土範圍與 count >= 3 的門檻都交給 SQL（ADR-0016）。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_accident_hotspots", return_value=self._hotspots(3)
            ) as m_db:
                with patch.object(ds, "set_cache"):
                    ds.get_accident_heatmap_data()

        kwargs = m_db.call_args.kwargs
        assert kwargs["lat_min"] == 21.755
        assert kwargs["lat_max"] == 25.93916
        assert kwargs["lon_min"] == 119.30083
        assert kwargs["lon_max"] == 124.56916
        assert kwargs["min_count"] == 3

    def test_熱點數超過上限時抽樣至上限(self):
        """抽樣是第三層減量，留在 pandas 做（ADR-0016 決策三）。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_accident_hotspots", return_value=self._hotspots(50)
            ):
                with patch.object(ds, "set_cache"):
                    result = ds.get_accident_heatmap_data(sample_size=10)

        assert len(result) == 10

    def test_抽樣結果可重現(self):
        """固定亂數種子，同一份資料每次抽出同一批點。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_accident_hotspots", return_value=self._hotspots(50)
            ):
                with patch.object(ds, "set_cache"):
                    first = ds.get_accident_heatmap_data(sample_size=10)
                    second = ds.get_accident_heatmap_data(sample_size=10)

        assert list(first["latitude"]) == list(second["latitude"])

    def test_熱點數未超過上限時不抽樣(self):
        """沒超過上限就整份回傳，不做多餘的抽樣。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds, "get_accident_hotspots", return_value=self._hotspots(5)
            ):
                with patch.object(ds, "set_cache"):
                    result = ds.get_accident_heatmap_data(sample_size=10)

        assert len(result) == 5

    def test_查無熱點時回傳空表且不寫快取(self):
        """查無資料是正常結果，但空表沒有快取的價值。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(ds, "get_accident_hotspots", return_value=pd.DataFrame()):
                with patch.object(ds, "set_cache") as m_set:
                    result = ds.get_accident_heatmap_data()

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        m_set.assert_not_called()

    def test_座標轉成_float64(self):
        """Decimal 型別進 folium 會出錯，SQL 回來的數值型別不保證是 float。"""
        df = pd.DataFrame({"latitude": ["25.0"], "longitude": ["121.0"], "count": [3]})
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(ds, "get_accident_hotspots", return_value=df):
                with patch.object(ds, "set_cache"):
                    result = ds.get_accident_heatmap_data()

        assert result["latitude"].dtype == "float64"
        assert result["longitude"].dtype == "float64"


class TestGetPedestrianTrend:
    """行人事故逐月趨勢：座標走 bind parameter，快取鍵隨座標而異。"""

    def test_不給座標時查全臺且不帶參數(self):
        """沒有座標就沒有 WHERE，參數也必須是空的。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                return_value=pd.DataFrame(),
            ) as m_db:
                with patch.object(ds, "set_cache"):
                    ds.get_pedestrian_trend()

        sql, params = m_db.call_args.args[0], m_db.call_args.args[1]
        assert "WHERE" not in sql
        assert params == {}

    def test_給座標時以方框過濾且座標走_bind_parameter(self):
        """座標的字面值不得出現在查詢字串裡（ADR-0009）。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                return_value=pd.DataFrame(),
            ) as m_db:
                with patch.object(ds, "set_cache"):
                    ds.get_pedestrian_trend(lat=25.0878, lon=121.5240, radius_km=0.5)

        sql, params = m_db.call_args.args[0], m_db.call_args.args[1]
        assert ":min_lat" in sql and ":max_lon" in sql
        # 座標的字面值不得出現在查詢字串裡
        assert "25.0878" not in sql
        assert "121.524" not in sql

        offset = 0.5 / 111.0
        assert params["min_lat"] == pytest.approx(25.0878 - offset)
        assert params["max_lat"] == pytest.approx(25.0878 + offset)
        assert params["min_lon"] == pytest.approx(121.5240 - offset)
        assert params["max_lon"] == pytest.approx(121.5240 + offset)

    def test_全臺與指定座標用不同的快取鍵(self):
        """兩種查詢範圍不同，共用一把鍵會讓彼此的結果互相污染。"""
        with patch.object(ds, "get_cache", return_value=None) as m_get:
            with patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                return_value=pd.DataFrame(),
            ):
                with patch.object(ds, "set_cache"):
                    ds.get_pedestrian_trend()
                    global_key = m_get.call_args.args[0]

                    ds.get_pedestrian_trend(lat=25.0878, lon=121.5240)
                    local_key = m_get.call_args.args[0]

        assert global_key == "analysis:pedestrian_trend_global_v2"
        assert local_key == "analysis:pedestrian_trend_local_v2:25.0878_121.524"

    def test_快取鍵與查詢範圍以同一組座標計算(self):
        """鍵取到小數第 4 位，範圍就得用同一組值算，否則鍵相同而範圍不同。"""
        with patch.object(ds, "get_cache", return_value=None) as m_get:
            with patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                return_value=pd.DataFrame(),
            ) as m_db:
                with patch.object(ds, "set_cache"):
                    ds.get_pedestrian_trend(lat=25.087811, lon=121.5240, radius_km=0.5)

        key = m_get.call_args.args[0]
        params = m_db.call_args.args[1]
        offset = 0.5 / 111.0

        assert key == "analysis:pedestrian_trend_local_v2:25.0878_121.524"
        assert params["min_lat"] == pytest.approx(25.0878 - offset)
        assert params["max_lat"] == pytest.approx(25.0878 + offset)

    def test_小數第五位不同的兩個座標查詢範圍相同(self):
        """既然共用一把快取鍵，兩者的查詢範圍就必須真的相同。"""
        ranges = []
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                return_value=pd.DataFrame(),
            ) as m_db:
                with patch.object(ds, "set_cache"):
                    ds.get_pedestrian_trend(lat=25.087811, lon=121.5240)
                    ranges.append(m_db.call_args.args[1])

                    ds.get_pedestrian_trend(lat=25.087849, lon=121.5240)
                    ranges.append(m_db.call_args.args[1])

        assert ranges[0] == ranges[1]

    @pytest.mark.parametrize(
        "lat, lon", [(25.0878, None), (None, 121.5240)], ids=["只給緯度", "只給經度"]
    )
    def test_座標只給一半時視為查全臺(self, lat, lon):
        """經緯度缺一就無法界定範圍，退回查全臺而不是拋出。"""
        with patch.object(ds, "get_cache", return_value=None) as m_get:
            with patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                return_value=pd.DataFrame(),
            ):
                with patch.object(ds, "set_cache"):
                    ds.get_pedestrian_trend(lat=lat, lon=lon)

        assert m_get.call_args.args[0] == "analysis:pedestrian_trend_global_v2"


class TestGetPedestrianStatsByRegionMonthly:
    """依地區與年月的統計：快取命中就不查 MySQL。"""

    def test_快取命中時不查_mysql(self):
        """快取命中就不查 MySQL，這是 read-through 的意義所在。"""
        with patch.object(ds, "get_cache", return_value=[{"region": "北部"}]):
            with patch.object(ds, "get_accident_table_pedestrian_involved_in") as m_db:
                result = ds.get_pedestrian_stats_by_region_monthly()

        m_db.assert_not_called()
        assert len(result) == 1

    def test_查詢依年月與地區分組(self):
        """分組欄位決定了回傳的粒度，前端依此畫圖。"""
        with patch.object(ds, "get_cache", return_value=None):
            with patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                return_value=pd.DataFrame(),
            ) as m_db:
                with patch.object(ds, "set_cache"):
                    ds.get_pedestrian_stats_by_region_monthly()

        sql = m_db.call_args.args[0]
        assert "GROUP BY accident_yearmonth, region" in sql
        assert "COUNT(distinct accident_id)" in sql
