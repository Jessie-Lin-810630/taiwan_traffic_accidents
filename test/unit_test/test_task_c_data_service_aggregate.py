"""驗證 DAG 呼叫的預計算：批次切分、方框查詢組裝與全臺總表聚合。

釘住三件事：

- 批次以 30 個夜市為單位，Redis 只存鍵不塞資料進 XCom
- 方框查詢的座標一律走 bind parameter，且每個夜市有獨立的佔位符編號（ADR-0009）
- 全臺總表的 PDI 計分與時段標籤，以及聚合完成後批次鍵會被清掉
"""

from unittest.mock import patch

import pandas as pd
import pytest

import src.task.core.c_data_service as ds


def _market(name: str, lat: float, lon: float, **overrides) -> dict:
    """一個夜市在批次中的樣子，邊界預設為中心點外擴 0.01 度。"""
    m = {
        "name": name,
        "city": "臺北市",
        "rating": 4.2,
        "lat": lat,
        "lon": lon,
        "n_lat": lat + 0.01,
        "s_lat": lat - 0.01,
        "e_lon": lon + 0.01,
        "w_lon": lon - 0.01,
    }
    m.update(overrides)
    return m


class TestBuildBatchBboxQuery:
    """方框查詢的組裝。"""

    def test_每個夜市有獨立編號的佔位符(self):
        """佔位符帶索引，同一批的夜市才不會互相覆蓋綁定值。"""
        batch = [_market("A", 25.0, 121.0), _market("B", 22.6, 120.3)]
        query, params = ds._build_batch_bbox_query(batch)

        assert ":min_lat_0" in query and ":max_lon_0" in query
        assert ":min_lat_1" in query and ":max_lon_1" in query
        assert len(params) == 8

    def test_各夜市的方框以_or_串成聯集(self):
        """一個批次一句查詢，各夜市的方框以 OR 串起來（ADR-0009）。"""
        batch = [_market("A", 25.0, 121.0), _market("B", 22.6, 120.3)]
        query, _ = ds._build_batch_bbox_query(batch)

        assert query.count(" OR ") == 1
        assert query.count("latitude BETWEEN") == 2

    def test_座標不內插進查詢字串(self):
        """座標一律走 bind parameter，不以字串內插回查詢。"""
        batch = [_market("A", 25.0878, 121.5240)]
        query, params = ds._build_batch_bbox_query(batch)

        assert "25.0878" not in query
        assert "121.524" not in query
        assert params["min_lat_0"] == pytest.approx(25.0878 - 3.0 / 111)
        assert params["max_lat_0"] == pytest.approx(25.0878 + 3.0 / 111)

    def test_半徑可調整且反映在參數上(self):
        """半徑換算成度數後進參數，不寫死在查詢裡。"""
        batch = [_market("A", 25.0, 121.0)]
        _, params = ds._build_batch_bbox_query(batch, radius_km=1.0)

        assert params["min_lat_0"] == pytest.approx(25.0 - 1.0 / 111)

    def test_夜市缺座標時拋出(self):
        """缺座標是上游資料的問題，這裡拋出而不是靜默跳過。"""
        with pytest.raises(KeyError):
            ds._build_batch_bbox_query([{"name": "沒座標"}])

    def test_空批次組不出條件(self):
        """空批次會產生 WHERE 後面沒東西的查詢，呼叫端不該傳空批次進來。"""
        query, params = ds._build_batch_bbox_query([])

        assert query.endswith("WHERE ")
        assert params == {}


class TestGetAndSliceNightmarketsMultibatches:
    """夜市清單切批次。"""

    def _markets_df(self, n: int) -> pd.DataFrame:
        """合成 n 個夜市的主檔，座標各異。"""
        return pd.DataFrame(
            [
                {
                    "nightmarket_name": f"夜市{i}",
                    "city": "臺北市",
                    "googlemap_rating": 4.0,
                    "latitude": 25.0 + i / 100,
                    "longitude": 121.0 + i / 100,
                    "northeast_latitude": 25.0 + i / 100 + 0.01,
                    "northeast_longitude": 121.0 + i / 100 + 0.01,
                    "southwest_latitude": 25.0 + i / 100 - 0.01,
                    "southwest_longitude": 121.0 + i / 100 - 0.01,
                }
                for i in range(n)
            ]
        )

    def test_每三十個夜市切成一批(self):
        """批次大小決定一次查詢涵蓋多少方框，也是 API 與記憶體的取捨點。"""
        with patch.object(
            ds, "get_all_nightmarkets", return_value=self._markets_df(65)
        ):
            with patch.object(ds, "set_cache") as m_set:
                keys = ds.get_and_slice_nightmarkets_multibatches()

        assert len(keys) == 3
        assert len(m_set.call_args_list[0].args[1]) == 30
        assert len(m_set.call_args_list[2].args[1]) == 5

    def test_只回傳鍵字串而不回傳資料(self):
        """回傳值會進 XCom，塞進整批夜市會讓 metadata 資料庫爆掉。"""
        with patch.object(ds, "get_all_nightmarkets", return_value=self._markets_df(3)):
            with patch.object(ds, "set_cache"):
                keys = ds.get_and_slice_nightmarkets_multibatches()

        assert all(isinstance(k, str) for k in keys)
        assert keys[0].startswith("xcom_claim_check:")
        assert keys[0].endswith(":batch_0")

    def test_每次呼叫的批次鍵都不重複(self):
        """鍵帶 uuid，兩次執行的批次不會互相覆蓋。"""
        with patch.object(ds, "get_all_nightmarkets", return_value=self._markets_df(3)):
            with patch.object(ds, "set_cache"):
                first = ds.get_and_slice_nightmarkets_multibatches()
                second = ds.get_and_slice_nightmarkets_multibatches()

        assert first != second

    def test_座標重複的夜市只留一個(self):
        """同座標的夜市算出來的周邊事故完全相同，重複查是浪費。"""
        df = self._markets_df(2)
        df.loc[1, "latitude"] = df.loc[0, "latitude"]
        df.loc[1, "longitude"] = df.loc[0, "longitude"]

        with patch.object(ds, "get_all_nightmarkets", return_value=df):
            with patch.object(ds, "set_cache") as m_set:
                ds.get_and_slice_nightmarkets_multibatches()

        assert len(m_set.call_args.args[1]) == 1

    def test_缺邊界座標時以中心點外擴五百公尺補上(self):
        """沒有 Google 回傳邊界的夜市仍要有方框，否則聚合階段會漏掉。"""
        df = self._markets_df(1)
        for col in (
            "northeast_latitude",
            "northeast_longitude",
            "southwest_latitude",
            "southwest_longitude",
        ):
            df[col] = None

        with patch.object(ds, "get_all_nightmarkets", return_value=df):
            with patch.object(ds, "set_cache") as m_set:
                ds.get_and_slice_nightmarkets_multibatches()

        market = m_set.call_args.args[1][0]
        assert market["n_lat"] == pytest.approx(market["lat"] + 0.005)
        assert market["s_lat"] == pytest.approx(market["lat"] - 0.005)
        assert market["e_lon"] == pytest.approx(market["lon"] + 0.005)
        assert market["w_lon"] == pytest.approx(market["lon"] - 0.005)

    def test_批次快取存活十二小時(self):
        """批次是暫存的寄物櫃，TTL 與其他預計算快取一致。"""
        with patch.object(ds, "get_all_nightmarkets", return_value=self._markets_df(1)):
            with patch.object(ds, "set_cache") as m_set:
                ds.get_and_slice_nightmarkets_multibatches()

        assert m_set.call_args.args[2] == 43200


class TestAggregateNationalMaster:
    """全臺總表的聚合與 PDI 計分。"""

    def _nearby_df(self, hour: int = 20, death: int = 1, injury: int = 2, **kw):
        """合成一列周邊事故，預設落在夜市方框內。"""
        row = {
            "accident_id": "A1",
            "accident_date": "2024-01-15",
            "accident_year": 2024,
            "accident_weekday": "星期一",
            "accident_hourtime": hour,
            "latitude": 25.0,
            "longitude": 121.0,
            "death_count": death,
            "injury_count": injury,
        }
        row.update(kw)
        return pd.DataFrame([row])

    def _run(self, market: dict, nearby: pd.DataFrame):
        """跑一次聚合，回傳 set_cache 的 mock 以便檢查各把鍵的內容。"""
        batch_key = "xcom_claim_check:uuid:batch_0"
        nearby_key = (
            f"traffic:nearby_v12:{market['lat']:.4f}_{market['lon']:.4f}_3.0_all_sample"
        )

        def fake_get_cache(key):
            """依鍵回傳批次清單或周邊事故，其餘鍵視為未命中。"""
            if key == batch_key:
                return [market]
            if key == nearby_key:
                return nearby
            return None

        with patch.object(ds, "get_cache", side_effect=fake_get_cache):
            with patch.object(ds, "set_cache") as m_set:
                with patch.object(ds, "delete_cache") as m_del:
                    ds.aggregate_national_master([batch_key])

        return m_set, m_del

    def _cached(self, m_set, key):
        """從 set_cache 的呼叫紀錄取出指定鍵寫入的內容。"""
        for call in m_set.call_args_list:
            if call.args[0] == key:
                return call.args[1]
        raise AssertionError(f"沒有寫入 {key}")

    def test_落在夜市方框內的事故進入總表並貼上夜市資訊(self):
        """總表要能回答「哪個夜市周邊」，因此每列都得帶夜市標籤。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df())

        master = self._cached(m_set, "market:national_master_df")
        assert len(master) == 1
        assert master["nightmarket_name"].iloc[0] == "士林夜市"
        assert master["nightmarket_city"].iloc[0] == "臺北市"
        assert master["googlemap_rating"].iloc[0] == 4.2

    def test_落在三公里內但方框外的事故被剔除(self):
        """粗篩是 3 公里方框，總表要的是夜市邊界加 500 公尺內的事故。"""
        market = _market("士林夜市", 25.0, 121.0)
        far = self._nearby_df()
        far.loc[0, "latitude"] = 25.02  # 超出 n_lat = 25.01

        with pytest.raises(RuntimeError, match="無法聚合全台總表"):
            self._run(market, far)

    @pytest.mark.parametrize(
        "hour, expected_weight",
        [(20, 1.5), (17, 1.5), (0, 1.5), (16, 1.0), (12, 1.0), (1, 1.0)],
    )
    def test_十七點後與零點的事故加權一點五倍(self, hour, expected_weight):
        """夜市的營業時段在夜間，PDI 以加權反映這段時間的風險。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df(hour=hour))

        master = self._cached(m_set, "market:national_master_df")
        assert master["weight"].iloc[0] == expected_weight

    def test_pdi_分數為死亡乘十加受傷乘二再乘權重(self):
        """計分公式是整個看板的核心指標，改動會讓歷史數字失去可比性。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df(hour=12, death=1, injury=2))

        master = self._cached(m_set, "market:national_master_df")
        assert master["severity"].iloc[0] == 14  # 1*10 + 2*2
        assert master["pdi_score"].iloc[0] == 14.0  # 白天不加權

    def test_夜間事故的_pdi_分數含加權(self):
        """夜間事故的分數是白天同樣傷亡的 1.5 倍。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df(hour=20, death=1, injury=2))

        master = self._cached(m_set, "market:national_master_df")
        assert master["pdi_score"].iloc[0] == pytest.approx(21.0)  # 14 * 1.5

    @pytest.mark.parametrize(
        "hour, expected_slot",
        [
            (6, "Day"),
            (12, "Day"),
            (17, "Day"),
            (18, "Night"),
            (5, "Night"),
            (0, "Night"),
        ],
    )
    def test_六點到十八點標為白天其餘為夜間(self, hour, expected_slot):
        """時段標籤的邊界值容易寫錯，逐一釘住。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df(hour=hour))

        macro = self._cached(m_set, "traffic:stats:audit_macro")
        assert macro["taiwan_markets_total"][0]["time_slot"] == expected_slot

    def test_補上年季月與星期等時間欄位(self):
        """前端的篩選器依賴這幾欄，缺一個就少一種切法。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df())

        master = self._cached(m_set, "market:national_master_df")
        assert master["Year"].iloc[0] == 2024
        assert master["Quarter"].iloc[0] == 1
        assert master["Month"].iloc[0] == 1
        assert master["Weekday"].iloc[0] == "星期一"

    def test_產出巨觀統計與各夜市的微觀統計(self):
        """三種快取各有消費端，少寫一把前端就會顯示請確認排程。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df())

        keys = [c.args[0] for c in m_set.call_args_list]
        assert "market:national_master_df" in keys
        assert "traffic:stats:audit_macro" in keys
        assert "traffic:stats:audit_market:士林夜市" in keys

    def test_巨觀統計含全臺與分縣市兩組(self):
        """巨觀 bundle 的結構是前端解析的契約。"""
        market = _market("士林夜市", 25.0, 121.0)
        m_set, _ = self._run(market, self._nearby_df())

        macro = self._cached(m_set, "traffic:stats:audit_macro")
        assert "taiwan_markets_total" in macro
        assert "city_markets_total" in macro
        assert "updated_at" in macro
        assert macro["taiwan_markets_total"][0]["acc_count"] == 1

    def test_聚合完成後清掉批次鍵(self):
        """批次資料是暫存的寄物櫃，用完要還，否則佔著 Redis 記憶體。"""
        market = _market("士林夜市", 25.0, 121.0)
        _, m_del = self._run(market, self._nearby_df())

        m_del.assert_called_once_with("xcom_claim_check:uuid:batch_0")

    def test_周邊事故快取為空時不進總表(self):
        """聚合不出資料時拋出，不讓 task 顯示成功（ADR-0003）。"""
        market = _market("士林夜市", 25.0, 121.0)

        with pytest.raises(RuntimeError, match="無法聚合全台總表"):
            self._run(market, pd.DataFrame())
