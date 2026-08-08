"""驗證 fact_accident_main 的 weather_record_id soft reference 是怎麼配對出來的。

配對鍵是「整點化時間 + 進位到氣象網格的經緯度」，三個維度只要有一個對不上，
merge 會一列都不回而且不會報錯 —— 這裡把那三個維度各釘一條測試。
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

import pandas as pd

from src.task.t_fact_hourly_weather import t_fact_main_ref_to_fact_weather

# e_get_all_acc_geo() 的產出形狀：時間已整點化並轉成字串，座標已進位。
DF_ACC = pd.DataFrame(
    {
        "accident_id": ["A1", "A2"],
        "lat_round": [25.05, 24.15],
        "lon_round": [121.55, 120.65],
        "approx_accident_datetime": [
            "2024-01-01 08:00:00",
            "2024-01-01 10:00:00",
        ],
    }
)

# fact_hourly_weather 回讀的形狀：DATETIME 是 datetime 物件、DECIMAL 是 Decimal。
DF_WEATHER = pd.DataFrame(
    {
        "weather_record_id": [1024, 2371],
        "observation_datetime": [
            datetime(2024, 1, 1, 8, 0, 0),
            datetime(2024, 1, 1, 10, 0, 0),
        ],
        "longitude_round": [Decimal("121.55"), Decimal("120.65")],
        "latitude_round": [Decimal("25.05"), Decimal("24.15")],
    }
)


def _run(df_acc=DF_ACC, df_weather=DF_WEATHER, target_year=2024):
    """以假查詢執行 t_fact_main_ref_to_fact_weather，回傳 (結果, 查詢呼叫)。"""
    with patch(
        "src.task.t_fact_hourly_weather.get_table_from_sqlserver",
        return_value=df_weather.copy(),
    ) as fake_query:
        result = t_fact_main_ref_to_fact_weather(
            target_year, df_acc.copy(), database="traffic_accident"
        )
    return result, fake_query


def test_配對成功時只回傳事故編號與天氣紀錄編號():
    """回傳的是 update_table 直接可用的兩欄對照表，多帶欄位會讓回填語句組錯。"""
    result, _ = _run()

    assert list(result.columns) == ["accident_id", "weather_record_id"]
    assert result.to_dict("records") == [
        {"accident_id": "A1", "weather_record_id": 1024},
        {"accident_id": "A2", "weather_record_id": 2371},
    ]


def test_事故時間以空白分隔而非_iso8601_的_t():
    """釘住時間格式：MySQL 的 DATETIME 轉字串是空白分隔，事故側必須同格式。

    事故側曾以 concat(date, "T", time) 組出 `2024-01-01T08:00:00`，
    與天氣側的 `2024-01-01 08:00:00` 對不上，merge 恆為 0 列且不報錯。
    """
    df_acc_with_t = DF_ACC.copy()
    df_acc_with_t["approx_accident_datetime"] = df_acc_with_t[
        "approx_accident_datetime"
    ].str.replace(" ", "T")

    result, _ = _run(df_acc=df_acc_with_t)

    assert len(result) == 0


def test_天氣側座標未進位時仍走同一支進位函式對得上():
    """依 ADR-0012，兩側座標都要走 round_to_weather_grid()，只要有一側用別的算法就全落空。"""
    df_weather_raw_coord = DF_WEATHER.copy()
    df_weather_raw_coord["longitude_round"] = [Decimal("121.5687"), Decimal("120.6412")]
    df_weather_raw_coord["latitude_round"] = [Decimal("25.0431"), Decimal("24.1553")]

    result, _ = _run(df_weather=df_weather_raw_coord)

    assert result["weather_record_id"].tolist() == [1024, 2371]


def test_時間或地點對不上的事故不會被回填():
    """Inner join：沒有對應天氣觀測的事故不該出現在結果中，也不該被填成任意值。"""
    df_acc_extra = pd.concat(
        [
            DF_ACC,
            pd.DataFrame(
                {
                    "accident_id": ["A3"],
                    "lat_round": [22.60],
                    "lon_round": [120.30],
                    "approx_accident_datetime": ["2024-06-15 03:00:00"],
                }
            ),
        ],
        ignore_index=True,
    )

    result, _ = _run(df_acc=df_acc_extra)

    assert "A3" not in result["accident_id"].tolist()


def test_查無天氣觀測時回傳空表且欄位仍在():
    """空結果不是故障（該年還沒抓到天氣），但欄位要在，下游才不會 KeyError。"""
    result, _ = _run(df_weather=DF_WEATHER.iloc[0:0])

    assert len(result) == 0
    assert list(result.columns) == ["accident_id", "weather_record_id"]


def test_年份走_bind_parameter_不內插():
    """依 ADR-0009，值一律走 bind parameter。"""
    _, fake_query = _run(target_year=2023)
    sql, params = fake_query.call_args[0]

    assert "2023" not in sql
    assert params == {
        "year_start": "2023-01-01 00:00:00",
        "next_year_start": "2024-01-01 00:00:00",
    }


def test_查詢條件不把觀測時間包進函式():
    """YEAR(observation_datetime) 會讓 idx_fact_hourly_weather_obt 失效，天氣表是百萬列等級。"""
    _, fake_query = _run()
    sql = fake_query.call_args[0][0]

    assert "YEAR(" not in sql.upper()
    assert "observation_datetime >=" in sql
