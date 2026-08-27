"""驗證預計算的查詢粒度以批次為單位，且輸出與逐夜市查詢等價（ADR-0009）。"""

from unittest.mock import patch

import pandas as pd
import pytest

import src.task.core.c_data_service as ds

# 三個夜市，前兩個的 3 公里框刻意重疊（同一個都會區），第三個遠在他方
BATCH = [
    {
        "name": "士林夜市",
        "lat": 25.0878,
        "lon": 121.5240,
        "city": "臺北市",
        "rating": 4.2,
        "n_lat": 25.09,
        "s_lat": 25.08,
        "e_lon": 121.53,
        "w_lon": 121.52,
    },
    {
        "name": "寧夏夜市",
        "lat": 25.0570,
        "lon": 121.5150,
        "city": "臺北市",
        "rating": 4.3,
        "n_lat": 25.06,
        "s_lat": 25.05,
        "e_lon": 121.52,
        "w_lon": 121.51,
    },
    {
        "name": "六合夜市",
        "lat": 22.6320,
        "lon": 120.3020,
        "city": "高雄市",
        "rating": 4.0,
        "n_lat": 22.64,
        "s_lat": 22.63,
        "e_lon": 120.31,
        "w_lon": 120.30,
    },
]


def _synthetic_table() -> pd.DataFrame:
    """合成一張分析表：含重疊區、框外、邊界、空座標與跨年度資料。"""
    rows = [
        # 只落在士林框內
        ("A1", 25.1000, 121.5300, 2025),
        # 落在士林與寧夏兩個框的重疊處 —— 舊實作會撈回來兩次
        ("A2", 25.0700, 121.5200, 2024),
        # 只落在寧夏框內
        ("A3", 25.0400, 121.5000, 2025),
        # 只落在六合框內
        ("A4", 22.6400, 120.3100, 2024),
        # 三個框都不在（台灣海峽）
        ("A5", 24.0000, 119.5000, 2025),
        # 恰好落在士林框的上邊界（BETWEEN 與 Series.between 都是閉區間）
        ("A6", 25.0878 + 3.0 / 111, 121.5240, 2025),
        # 座標缺值：SQL 的 BETWEEN 排除 NULL，pandas 的 between 排除 NaN
        ("A7", None, 121.5240, 2025),
    ]
    return pd.DataFrame(
        rows, columns=["accident_id", "latitude", "longitude", "accident_year"]
    )


def _fake_mysql(df_table: pd.DataFrame):
    """以 bind parameter 驅動的假 MySQL：只認 params，不解析 SQL 字串。

    因此若實作退回用 f-string 把座標內插進查詢字串，params 會是空的，
    這個假資料庫會直接讓測試失敗 —— 這正是 ADR-0009 子決策 3 要釘住的。
    """

    def _query(dql_str, params=None):
        assert params, "座標必須以 bind parameter 傳遞，不可 f-string 內插（ADR-0009）"
        mask = pd.Series(False, index=df_table.index)
        for i in range(len(params) // 4):
            mask |= df_table["latitude"].between(
                params[f"min_lat_{i}"], params[f"max_lat_{i}"]
            ) & df_table["longitude"].between(
                params[f"min_lon_{i}"], params[f"max_lon_{i}"]
            )
        return df_table[mask].reset_index(drop=True)

    return _query


def _粗篩結果_改動前的算法(df_table: pd.DataFrame, nightmarket: dict) -> pd.DataFrame:
    """把 ADR-0009 之前的粗篩邏輯原封不動抄一份過來。

    舊實作是「逐夜市各發一次 SQL，撈回該夜市 3 公里框內的列，
    裁欄位後把三個座標／年度欄轉成數值」。這裡在同一張假表上算出
    「改動前會寫進 Redis 的那份表」，用來跟新實作實際寫進去的內容逐列比對。
    """
    max_offset = (3000 / 1000) / 111  # 原始寫法，刻意不改寫成 3.0 / 111
    nm_lat, nm_lon = nightmarket["lat"], nightmarket["lon"]

    df = df_table[
        df_table["latitude"].between(nm_lat - max_offset, nm_lat + max_offset)
        & df_table["longitude"].between(nm_lon - max_offset, nm_lon + max_offset)
    ].reset_index(drop=True)

    valid_cols = [c for c in ds.ACCIDENT_MAP_COLUMNS if c in df.columns]
    df = df[valid_cols].copy()

    if not df.empty:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["accident_year"] = pd.to_numeric(df["accident_year"], errors="coerce")
    return df


def _run(batch=None, table=None, **kwargs) -> dict:
    """跑一次預計算，回傳 {cache_key: 寫入的值}。"""
    batch = BATCH if batch is None else batch
    table = _synthetic_table() if table is None else table
    written = {}

    with (
        patch.object(ds, "get_cache", return_value=batch),
        patch.object(
            ds,
            "get_accident_table_pedestrian_involved_in",
            side_effect=_fake_mysql(table),
        ),
        patch.object(
            ds, "set_cache", side_effect=lambda k, v, ttl=None: written.update({k: v})
        ),
    ):
        ds.cal_accidents_nearby_nightmarket("batch_key", **kwargs)

    return written


class TestOutputEquivalence:
    """效能重構的前提：輸出必須與改動前逐列相同。"""

    @pytest.mark.parametrize("nightmarket", BATCH, ids=lambda nm: nm["name"])
    def test_粗篩結果與逐夜市查詢的舊邏輯逐列相同(self, nightmarket):
        """與改動前的邏輯跑同一份假資料，輸出必須逐列相同（ADR-0009 子決策 8）。"""
        written = _run()
        key = (
            f"mart:pedestrian_nearby_market:{nightmarket['lat']:.4f}_"
            f"{nightmarket['lon']:.4f}_3.0_all_sample"
        )

        pd.testing.assert_frame_equal(
            written[key], _粗篩結果_改動前的算法(_synthetic_table(), nightmarket)
        )

    def test_重疊區的事故同時出現在兩個夜市的快取中(self):
        """重疊只在傳輸層被消除，語意上該事故仍屬於兩個夜市。"""
        written = _run()
        shilin = written[
            "mart:pedestrian_nearby_market:25.0878_121.5240_3.0_all_sample"
        ]
        ningxia = written[
            "mart:pedestrian_nearby_market:25.0570_121.5150_3.0_all_sample"
        ]

        assert "A2" in set(shilin["accident_id"])
        assert "A2" in set(ningxia["accident_id"])

    def test_框外與空座標的事故不進任何快取(self):
        """SQL 的 BETWEEN 排除 NULL，pandas 的 between 排除 NaN，兩邊一致。"""
        written = _run()
        all_ids = set()
        for df in written.values():
            all_ids |= set(df["accident_id"])

        assert "A5" not in all_ids  # 台灣海峽
        assert "A7" not in all_ids  # 座標缺值
        assert "A6" in all_ids  # 邊界值必須包含


class TestQueryGrain:
    """查詢的粒度必須與使用它的粒度一致（ADR-0009 決策本體）。"""

    def test_一個批次只發一次查詢(self):
        """三個夜市、一次查詢 —— 退回 N+1 會立刻在這裡紅燈。"""
        with (
            patch.object(ds, "get_cache", return_value=BATCH),
            patch.object(
                ds,
                "get_accident_table_pedestrian_involved_in",
                side_effect=_fake_mysql(_synthetic_table()),
            ) as mock_query,
            patch.object(ds, "set_cache"),
        ):
            ds.cal_accidents_nearby_nightmarket("batch_key")

        assert mock_query.call_count == 1

    def test_查詢以聯集涵蓋批次內每個夜市(self):
        """每個夜市貢獻一組 4 個綁定參數，缺一個就有夜市被漏掉。"""
        query, params = ds._build_query_market_batch_nearby_box(BATCH)

        assert query.count(" OR ") == len(BATCH) - 1
        assert len(params) == len(BATCH) * 4
        for i, nightmarket in enumerate(BATCH):
            offset = 3.0 / 111
            assert params[f"min_lat_{i}"] == pytest.approx(nightmarket["lat"] - offset)
            assert params[f"max_lon_{i}"] == pytest.approx(nightmarket["lon"] + offset)

    def test_查詢字串不含座標字面值(self):
        """座標一律走 bind parameter（ADR-0009 子決策 3）。"""
        query, _ = ds._build_query_market_batch_nearby_box(BATCH)

        assert "25.0878" not in query
        assert "121.5" not in query
        assert ":min_lat_0" in query


class TestCacheKeys:
    """契約 key 的存在不能取決於呼叫端傳了什麼參數（ADR-0009 子決策 5）。"""

    def test_預設參數下每個夜市只寫一把_key(self):
        """細篩迴圈唯一那圈與粗篩是同一把 key、同一份資料，不該寫第二次。"""
        written = _run()

        assert len(written) == len(BATCH)
        for nightmarket in BATCH:
            assert (
                f"mart:pedestrian_nearby_market:{nightmarket['lat']:.4f}_"
                f"{nightmarket['lon']:.4f}_3.0_all_sample"
            ) in written

    def test_指定額外半徑與年份時才產生額外的_key(self):
        """參數保留是為了多半徑／分年度預計算，功能必須真的還在。"""
        written = _run(radius_m_list=[500, 3000], year_targets=["all_sample", 2025])

        shilin = "mart:pedestrian_nearby_market:25.0878_121.5240"
        # 粗篩契約 key + 0.5_all_sample + 0.5_2025 + 3.0_2025；3.0_all_sample 只寫一次
        assert f"{shilin}_3.0_all_sample" in written
        assert f"{shilin}_0.5_all_sample" in written
        assert f"{shilin}_0.5_2025" in written
        assert f"{shilin}_3.0_2025" in written
        assert len(written) == len(BATCH) * 4

    def test_契約_key_不受_radius_參數影響(self):
        """aggregate_national_master() 只讀 _3.0_all_sample，它必須無條件存在。"""
        written = _run(radius_m_list=[500], year_targets=[2025])

        for nightmarket in BATCH:
            assert (
                f"mart:pedestrian_nearby_market:{nightmarket['lat']:.4f}_"
                f"{nightmarket['lon']:.4f}_3.0_all_sample"
            ) in written


def test_預設參數不是可變物件():
    """可變預設值會在多次呼叫間共享同一個 list（ADR-0009 子決策 4）。"""
    import inspect

    defaults = inspect.signature(ds.cal_accidents_nearby_nightmarket).parameters
    assert defaults["radius_m_list"].default is None
    assert defaults["year_targets"].default is None


def test_逐夜市迴圈不再空等():
    """查詢移出迴圈後，迴圈裡已經沒有查詢來回，節流的對象不存在（ADR-0009 子決策 7）。"""
    assert not hasattr(ds, "time"), "c_data_service 不應再匯入 time"
