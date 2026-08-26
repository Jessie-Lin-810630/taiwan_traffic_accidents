"""驗證夜市 JSON 的讀取契約，以及清洗的七支私有函式。

讀取契約要釘住的是失敗語意（ADR-0019）：

- 缺一個必要欄位（25%）記 warning 並繼續，缺兩個（50%）記 error 並拋 `ValueError`
- 缺 `result` 欄位視為四個必要欄位全缺，走同一條門檻，不再拋難懂的 `KeyError`
- 驗證在名稱過濾之前，母體是全部項目而非過濾後的夜市

七支清洗函式都是純轉換函式，沒有故障語意可釘，測試集中在四件事：

- 快樂路徑：正常的 Google Maps 回應片段被拆成正確的欄位
- 缺漏填補：來源缺欄時填的是各自約定的值（`None` 或說明字串），不是空字串
- 分支窮舉：營業時間的狀況 A～E、名稱的括號補述，每個分支各一個代表案例
- 型別保證：座標是 `float` 或 `None`、zipcode 保持字串、評分是 `float` 或 `None`

最後一組釘住 `_t_clean_one_night_market()` 與六支子函式的契約：它必須確實呼叫
六支，並把「對整個夜市都相同」的欄位貼到每一列上。
"""

import json
from unittest.mock import patch

import pandas as pd
import pytest

from src.task.e_crawling_nightmarket import cities_per_region
from src.task.t_fact_night_markets import (
    _clean_business_datetime,
    _clean_googlemap_rating,
    _clean_googlemap_url,
    _clean_night_market_address,
    _clean_night_market_geometry_location,
    _clean_night_market_name,
    _t_clean_one_night_market,
    read_googlemap_responsed_json,
)

# 一份完整的 Google Maps 回應片段，欄位齊全、營業時間走狀況 E
A_NIGHT_MARKET = {
    "name": "士林夜市",
    "rating": 4.2,
    "formatted_address": "111台灣臺北市士林區大東路",
    "url": "https://maps.google.com/?cid=123",
    "geometry": {
        "location": {"lat": 25.0881, "lng": 121.5243},
        "viewport": {
            "northeast": {"lat": 25.0890, "lng": 121.5252},
            "southwest": {"lat": 25.0872, "lng": 121.5234},
        },
    },
    "opening_hours": {
        "periods": [
            {"open": {"day": 3, "time": "1700"}, "close": {"day": 3, "time": "2330"}}
        ]
    },
}


def _periods(*periods):
    """把幾段 periods 包成 `_clean_business_datetime()` 吃的形狀。"""
    return {"opening_hours": {"periods": list(periods)}}


def _write_json(tmp_path, results, name="night_markets.json"):
    """把幾份 `result` 包成 Places API 回應的形狀並寫成檔案。

    傳 `None` 代表這個項目連 `result` 都沒有，用來模擬結構已變的回應。
    """
    items = [
        {"status": "OK"} if r is None else {"status": "OK", "result": r}
        for r in results
    ]
    path = tmp_path / name
    path.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _without(*keys):
    """複製一份完整夜市資料，去掉指定的欄位。"""
    return {k: v for k, v in A_NIGHT_MARKET.items() if k not in keys}


class Test讀取與欄位驗證:
    """read_googlemap_responsed_json 的失敗語意，參考 ADR-0019。"""

    def test_欄位齊全時取出含夜市或商圈的項目(self, tmp_path):
        """快樂路徑：驗證通過，名稱過濾照舊運作。"""
        path = _write_json(
            tmp_path,
            [
                A_NIGHT_MARKET,
                dict(A_NIGHT_MARKET, name="逢甲商圈"),
                dict(A_NIGHT_MARKET, name="臺北車站"),
            ],
        )

        got = read_googlemap_responsed_json(path)

        assert [r["name"] for r in got] == ["士林夜市", "逢甲商圈"]

    def test_欄位齊全時不發_warning(self, tmp_path, caplog):
        """沒有缺欄就不該產生噪音。"""
        path = _write_json(tmp_path, [A_NIGHT_MARKET])

        with caplog.at_level("WARNING"):
            read_googlemap_responsed_json(path)

        assert caplog.text == ""

    def test_缺一個必要欄位記_warning_但不中止(self, tmp_path, caplog):
        """缺 1 個 = 25%，未達 30% 門檻，這一筆照常留下。"""
        path = _write_json(tmp_path, [_without("opening_hours")])

        with caplog.at_level("WARNING"):
            got = read_googlemap_responsed_json(path)

        assert len(got) == 1
        assert "opening_hours" in caplog.text

    def test_缺兩個必要欄位就拋出(self, tmp_path):
        """缺 2 個 = 50%，超過門檻。這是 ADR-0019 的核心判準。"""
        path = _write_json(tmp_path, [_without("opening_hours", "geometry")])

        with pytest.raises(ValueError):
            read_googlemap_responsed_json(path)

    def test_缺兩個必要欄位時記_error_而不是_warning(self, tmp_path, caplog):
        """Schema 變動重試無效，是永久性故障（ADR-0006 第 5 條）。"""
        path = _write_json(tmp_path, [_without("opening_hours", "geometry")])

        with caplog.at_level("WARNING"), pytest.raises(ValueError):
            read_googlemap_responsed_json(path)

        assert [r.levelname for r in caplog.records] == ["ERROR"]

    def test_沒有_result_的項目視為四個必要欄位全缺(self, tmp_path):
        """`{"status": "OK"}` 這種回應走同一條門檻，不再拋難懂的 KeyError。"""
        path = _write_json(tmp_path, [None, None, None])

        with pytest.raises(ValueError) as excinfo:
            read_googlemap_responsed_json(path)

        message = str(excinfo.value)
        for key in ("name", "formatted_address", "geometry", "opening_hours"):
            assert key in message

    def test_錯誤訊息帶得出診斷資訊(self, tmp_path):
        """訊息要能回答「幾筆出事、缺哪些欄位」，否則等於沒有訊息。"""
        path = _write_json(tmp_path, [_without("name", "geometry"), A_NIGHT_MARKET])

        with pytest.raises(ValueError) as excinfo:
            read_googlemap_responsed_json(path)

        message = str(excinfo.value)
        assert "共 2 筆" in message
        assert "1 筆" in message
        assert "name" in message and "geometry" in message

    def test_rating_與_url_不算必要欄位(self, tmp_path, caplog):
        """這兩個不列入必要，冷門夜市本來就可能沒有評分。"""
        path = _write_json(tmp_path, [_without("rating", "url")])

        with caplog.at_level("WARNING"):
            got = read_googlemap_responsed_json(path)

        assert len(got) == 1
        assert caplog.text == ""

    def test_驗證發生在名稱過濾之前(self, tmp_path):
        """母體是全部項目，非夜市的項目結構壞掉照樣要攔下來。

        若驗證放在過濾之後，這個非夜市項目會先被丟掉，schema 已變的事實就被吞掉了。
        """
        broken_non_night_market = {
            k: v
            for k, v in A_NIGHT_MARKET.items()
            if k not in ("opening_hours", "geometry")
        }
        broken_non_night_market["name"] = "臺北車站"
        path = _write_json(tmp_path, [A_NIGHT_MARKET, broken_non_night_market])

        with pytest.raises(ValueError):
            read_googlemap_responsed_json(path)

    def test_缺_name_的項目被濾掉而不是拋_TypeError(self, tmp_path):
        """缺 name 只有 25%，未達門檻；但沒有名字就無從判斷是不是夜市。

        舊實作會在 `"夜市" in None` 拋 TypeError。
        """
        path = _write_json(tmp_path, [_without("name"), A_NIGHT_MARKET])

        got = read_googlemap_responsed_json(path)

        assert [r["name"] for r in got] == ["士林夜市"]

    def test_掃完全部才拋而不是遇到第一筆就中斷(self, tmp_path):
        """訊息要涵蓋全部超標的筆數，不能只報第一筆（ADR-0019 決策六）。"""
        broken = _without("name", "geometry")
        path = _write_json(tmp_path, [broken, A_NIGHT_MARKET, broken])

        with pytest.raises(ValueError) as excinfo:
            read_googlemap_responsed_json(path)

        assert "2 筆" in str(excinfo.value)


class Test名稱清理:
    """括號補述該不該留，取決於括號內有沒有「夜市」或「商圈」。"""

    def test_括號內沒有夜市或商圈時整段去掉(self):
        """「基隆廟口夜市(仁三路)」的括號是路名補述，不屬於名稱。"""
        got = _clean_night_market_name({"name": "基隆廟口夜市(仁三路)"})

        assert got == {"nightmarket_name": "基隆廟口夜市"}

    def test_括號內有夜市字樣時整段保留(self):
        """括號本身是別名的一部分就不能砍，否則會失去辨識度。"""
        got = _clean_night_market_name({"name": "逢甲夜市（文華路夜市）"})

        assert got == {"nightmarket_name": "逢甲夜市（文華路夜市）"}

    def test_全形與半形括號都認得(self):
        """來源兩種括號都出現過，只處理其中一種會漏掉另一半的資料。"""
        got = _clean_night_market_name({"name": "羅東夜市（民生市場）"})

        assert got == {"nightmarket_name": "羅東夜市"}

    def test_沒有括號時原樣保留(self):
        """最常見的情況不該被動到。"""
        got = _clean_night_market_name({"name": "士林夜市"})

        assert got == {"nightmarket_name": "士林夜市"}

    def test_前後空白被去掉(self):
        """名稱是下游的顯示欄位，帶空白會讓相同夜市看起來不同。"""
        got = _clean_night_market_name({"name": "  寧夏夜市  "})

        assert got == {"nightmarket_name": "寧夏夜市"}


class Test地址拆解:
    """從一整串格式化地址拆出五個欄位，拆不到時填說明字串而非空值。"""

    def test_完整地址拆出五個欄位(self):
        """快樂路徑：地區由縣市反查、行政區取「區」字之前、zipcode 是開頭數字。"""
        got = _clean_night_market_address(
            {"formatted_address": "200台灣基隆市仁愛區玉田里仁三路"}, cities_per_region
        )

        assert got == {
            "region": "北部",
            "city": "基隆市",
            "district": "仁愛區",
            "zipcode": "200",
            "area_road": "200臺灣基隆市仁愛區玉田里仁三路",
        }

    def test_台一律被統一成臺(self):
        """來源兩種寫法混用，不統一會讓同一個縣市比對不到自己。"""
        got = _clean_night_market_address(
            {"formatted_address": "111台灣台北市士林區大東路"}, cities_per_region
        )

        assert got["city"] == "臺北市"
        assert "台" not in got["area_road"]

    def test_地區由縣市反查而不是另外比對(self):
        """臺東縣屬東部，region 的唯一來源是 cities_per_region 的分組。"""
        got = _clean_night_market_address(
            {"formatted_address": "950臺東縣臺東市正氣路"}, cities_per_region
        )

        assert got["region"] == "東部"
        assert got["city"] == "臺東縣"

    def test_比對不到的欄位填說明字串而不是空值(self):
        """下游要能分辨「沒有這筆資料」與「清理邏輯漏掉」，空字串做不到。"""
        got = _clean_night_market_address(
            {"formatted_address": "日本東京都"}, cities_per_region
        )

        assert got == {
            "region": "無匹配地區資訊",
            "city": "無匹配縣市資訊",
            "district": "無匹配第二、三行政區",
            "zipcode": "無匹配郵遞區號",
            "area_road": "日本東京都",
        }

    def test_完全沒有地址欄位時不炸掉(self):
        """來源缺 formatted_address 時走預設值，整支仍要回傳完整的五個欄位。"""
        got = _clean_night_market_address({}, cities_per_region)

        assert got["area_road"] == "無地址資訊"
        assert set(got) == {"region", "city", "district", "zipcode", "area_road"}

    def test_門牌號碼不會被當成_zipcode(self):
        """「1號」的數字緊鄰「號」，是門牌不是郵遞區號，這是那串 lookbehind 的用意。"""
        got = _clean_night_market_address(
            {"formatted_address": "臺北市大安區忠孝東路四段1號"}, cities_per_region
        )

        assert got["zipcode"] == "無匹配郵遞區號"

    def test_zipcode_保持字串而不是數字(self):
        """開頭是 0 的郵遞區號轉成數字會掉一位，型別必須是字串。"""
        got = _clean_night_market_address(
            {"formatted_address": "200臺灣基隆市仁愛區仁三路"}, cities_per_region
        )

        assert isinstance(got["zipcode"], str)

    def test_行政區去掉前面的縣市贅字(self):
        """取「區」之前會連縣市一起帶進來，要砍到只剩行政區名。"""
        got = _clean_night_market_address(
            {"formatted_address": "300新竹市東區中央路"}, cities_per_region
        )

        assert got["district"] == "東區"

    def test_街道地址保留原樣以利追溯(self):
        """area_road 是回頭檢查前述清理有沒有漏的依據，不做額外裁切。"""
        address = "111臺灣臺北市士林區大東路"
        got = _clean_night_market_address(
            {"formatted_address": address}, cities_per_region
        )

        assert got["area_road"] == address


class Test座標清理:
    """中心點與東北、西南兩個邊界端點，取不到一律 None。"""

    def test_取出中心點與兩個邊界端點(self):
        """快樂路徑：六個座標各自從 geometry 的兩層結構取出。"""
        got = _clean_night_market_geometry_location(A_NIGHT_MARKET)

        assert got == {
            "latitude": 25.0881,
            "longitude": 121.5243,
            "northeast_latitude": 25.0890,
            "northeast_longitude": 121.5252,
            "southwest_latitude": 25.0872,
            "southwest_longitude": 121.5234,
        }

    def test_缺_geometry_時六個座標都是_None(self):
        """取不到座標要是 None，不能以 0 代替，0 是赤道上的真實座標。"""
        got = _clean_night_market_geometry_location({})

        assert set(got.values()) == {None}

    def test_只有中心點沒有_viewport_時邊界是_None(self):
        """兩層結構各自獨立，缺一層不該讓另一層也失效。"""
        got = _clean_night_market_geometry_location(
            {"geometry": {"location": {"lat": 25.0, "lng": 121.0}}}
        )

        assert got["latitude"] == 25.0
        assert got["northeast_latitude"] is None
        assert got["southwest_longitude"] is None

    def test_座標是_float_而不是字串(self):
        """來源若給字串，寫進 MySQL 的 decimal 欄位會出問題。"""
        got = _clean_night_market_geometry_location(
            {"geometry": {"location": {"lat": "25.0881", "lng": "121.5243"}}}
        )

        assert isinstance(got["latitude"], float)
        assert isinstance(got["longitude"], float)


class Test營業時間拆列:
    """把一週的營業時段整理成一天一列，五種情況各一個代表案例。"""

    def test_狀況A_全年無休展開成七天(self):
        """來源沒有 close 就是 24 小時營業，七天都填滿整日。"""
        got = _clean_business_datetime(_periods({"open": {"day": 0, "time": "0000"}}))

        assert len(got) == 7
        assert {r["business_days_weekday"] for r in got} == {
            "星期日",
            "星期一",
            "星期二",
            "星期三",
            "星期四",
            "星期五",
            "星期六",
        }
        assert all(r["business_hours_opening"] == "00:00:00" for r in got)
        assert all(r["business_hours_closing"] == "23:59:59" for r in got)

    def test_狀況B_平日跨夜拆成兩列(self):
        """週日 16:00 到週一 02:00 要拆成當天到 23:59:59 與隔天 00:00:00 起。"""
        got = _clean_business_datetime(
            _periods(
                {
                    "open": {"day": 0, "time": "1600"},
                    "close": {"day": 1, "time": "0200"},
                }
            )
        )

        assert got == [
            {
                "business_days_weekday": "星期日",
                "business_hours_opening": "16:00:00",
                "business_hours_closing": "23:59:59",
            },
            {
                "business_days_weekday": "星期一",
                "business_hours_opening": "00:00:00",
                "business_hours_closing": "02:00:00",
            },
        ]

    def test_狀況C_週六跨到週日同樣拆成兩列(self):
        """結束日的星期序號比開始日小，不能因此被當成沒有跨夜。"""
        got = _clean_business_datetime(
            _periods(
                {
                    "open": {"day": 6, "time": "1700"},
                    "close": {"day": 0, "time": "0100"},
                }
            )
        )

        assert got == [
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

    def test_狀況D_結束標成隔天零點不算跨夜(self):
        """隔天 00:00 收攤等於當天營業到底，不該在隔天多出一列零長度的時段。"""
        got = _clean_business_datetime(
            _periods(
                {
                    "open": {"day": 6, "time": "1600"},
                    "close": {"day": 0, "time": "0000"},
                }
            )
        )

        assert got == [
            {
                "business_days_weekday": "星期六",
                "business_hours_opening": "16:00:00",
                "business_hours_closing": "23:59:59",
            }
        ]

    def test_狀況E_當天開當天收只有一列(self):
        """最常見的情況，開始與結束都在同一天。"""
        got = _clean_business_datetime(
            _periods(
                {
                    "open": {"day": 3, "time": "1700"},
                    "close": {"day": 3, "time": "2330"},
                }
            )
        )

        assert got == [
            {
                "business_days_weekday": "星期三",
                "business_hours_opening": "17:00:00",
                "business_hours_closing": "23:30:00",
            }
        ]

    def test_四碼時間被轉成時分秒(self):
        """來源是 "1630" 這種四碼字串，資料表要的是 HH:MM:SS。"""
        got = _clean_business_datetime(
            _periods(
                {
                    "open": {"day": 1, "time": "0630"},
                    "close": {"day": 1, "time": "1145"},
                }
            )
        )

        assert got[0]["business_hours_opening"] == "06:30:00"
        assert got[0]["business_hours_closing"] == "11:45:00"

    def test_沒有營業時間資訊時回傳空清單(self):
        """來源缺 opening_hours 時不該炸掉，這個夜市就是沒有時段可展開。"""
        assert _clean_business_datetime({}) == []


class Test評分與網址:
    """兩支最單純的取值，重點在缺漏時填什麼與型別。"""

    def test_取出評分並轉成_float(self):
        """評分要進 MySQL 的浮點欄位，來源給整數時也要是 float。"""
        got = _clean_googlemap_rating({"rating": 4})

        assert got == {"googlemap_rating": 4.0}
        assert isinstance(got["googlemap_rating"], float)

    def test_沒有評分時是_None_而不是零分(self):
        """0.0 代表「評分是零分」，與「沒有人評分」是兩件事。"""
        assert _clean_googlemap_rating({}) == {"googlemap_rating": None}

    def test_取出地圖網址(self):
        """快樂路徑。"""
        got = _clean_googlemap_url({"url": "https://maps.google.com/?cid=123"})

        assert got == {"url_to_googlemap": "https://maps.google.com/?cid=123"}

    def test_沒有網址時填未取得(self):
        """這一欄約定用說明字串而不是 None，與座標的約定不同。"""
        assert _clean_googlemap_url({}) == {"url_to_googlemap": "未取得"}


class Test組裝一個夜市:
    """_t_clean_one_night_market 與六支子函式的契約。"""

    def test_以營業時段為骨架展開成多列(self):
        """一個跨夜的夜市會展開成兩列，列數由營業時段決定。"""
        nm = dict(
            A_NIGHT_MARKET,
            opening_hours={
                "periods": [
                    {
                        "open": {"day": 0, "time": "1600"},
                        "close": {"day": 1, "time": "0200"},
                    }
                ]
            },
        )

        got = _t_clean_one_night_market(nm, cities_per_region)

        assert len(got) == 2
        assert [r["business_days_weekday"] for r in got] == ["星期日", "星期一"]

    def test_夜市層級的欄位每一列都相同(self):
        """名稱、地址、座標對整個夜市只有一份，展開後每列都要帶上同樣的值。"""
        nm = dict(
            A_NIGHT_MARKET,
            opening_hours={
                "periods": [
                    {
                        "open": {"day": 0, "time": "1600"},
                        "close": {"day": 1, "time": "0200"},
                    }
                ]
            },
        )

        got = _t_clean_one_night_market(nm, cities_per_region)

        for column in ("nightmarket_name", "city", "latitude", "googlemap_rating"):
            assert len({r[column] for r in got}) == 1

    def test_一列帶齊十七個欄位(self):
        """欄位少一個就會在 loader 那端才爆，這裡先釘住輸出的形狀。"""
        got = _t_clean_one_night_market(A_NIGHT_MARKET, cities_per_region)

        assert set(got[0]) == {
            "business_days_weekday",
            "business_hours_opening",
            "business_hours_closing",
            "nightmarket_name",
            "region",
            "city",
            "district",
            "zipcode",
            "area_road",
            "latitude",
            "longitude",
            "northeast_latitude",
            "northeast_longitude",
            "southwest_latitude",
            "southwest_longitude",
            "url_to_googlemap",
            "googlemap_rating",
        }

    def test_六支子函式都被呼叫過(self):
        """組裝函式本身不做清理，任何一支沒被叫到就代表那個面向漏掉了。"""
        module = "src.task.t_fact_night_markets"
        with (
            patch(
                f"{module}._clean_night_market_name",
                return_value={"nightmarket_name": "X"},
            ) as name,
            patch(
                f"{module}._clean_night_market_address",
                return_value={
                    "region": "北部",
                    "city": "臺北市",
                    "district": "士林區",
                    "zipcode": "111",
                    "area_road": "大東路",
                },
            ) as address,
            patch(
                f"{module}._clean_night_market_geometry_location",
                return_value={
                    "latitude": 25.0,
                    "longitude": 121.0,
                    "northeast_latitude": None,
                    "northeast_longitude": None,
                    "southwest_latitude": None,
                    "southwest_longitude": None,
                },
            ) as geometry,
            patch(
                f"{module}._clean_business_datetime",
                return_value=[
                    {
                        "business_days_weekday": "星期三",
                        "business_hours_opening": "17:00:00",
                        "business_hours_closing": "23:30:00",
                    }
                ],
            ) as datetime_,
            patch(
                f"{module}._clean_googlemap_rating",
                return_value={"googlemap_rating": 4.2},
            ) as rating,
            patch(
                f"{module}._clean_googlemap_url", return_value={"url_to_googlemap": "u"}
            ) as url,
        ):
            _t_clean_one_night_market(A_NIGHT_MARKET, cities_per_region)

        for mocked in (name, geometry, datetime_, rating, url):
            mocked.assert_called_once_with(A_NIGHT_MARKET)
        address.assert_called_once_with(A_NIGHT_MARKET, cities_per_region)

    def test_地區對照表原樣傳給地址清理(self):
        """只有地址那支需要 cities_per_region，傳錯就會整批算不出 region。"""
        module = "src.task.t_fact_night_markets"
        another_map = {"測試區": ["測試市"]}
        with patch(
            f"{module}._clean_night_market_address",
            return_value={
                "region": "測試區",
                "city": "測試市",
                "district": "d",
                "zipcode": "1",
                "area_road": "r",
            },
        ) as address:
            _t_clean_one_night_market(A_NIGHT_MARKET, another_map)

        address.assert_called_once_with(A_NIGHT_MARKET, another_map)

    def test_回傳的是_records_形式的_list(self):
        """下游用 pd.DataFrame(all_records) 收，形狀必須是 list[dict]。"""
        got = _t_clean_one_night_market(A_NIGHT_MARKET, cities_per_region)

        assert isinstance(got, list)
        assert all(isinstance(r, dict) for r in got)
        assert not isinstance(got, pd.DataFrame)
