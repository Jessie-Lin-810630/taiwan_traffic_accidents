"""驗證 ADR-0013 的斷點設計：檔名承載月份、完成判定、批次以（觀測點, 月）為單位。"""

from datetime import date, datetime
from unittest.mock import patch

import pandas as pd

from src.task.e_crawling_weather import (
    TAIPEI,
    _batch_object,
    blob_file_name,
    e_crawler_weatherapi,
    is_month_complete,
    month_date_range,
    months_to_fetch,
    prep_batch_plan,
    weather_data_prefix,
)

# 兩個觀測點，經緯度已進位到 0.05 度網格
DF_TWO_POINTS = pd.DataFrame(
    {"lat_round": [24.15, 25.05], "lon_round": [121.55, 121.50]}
)

HOURLY_KEYS = [
    "time",
    "temperature_2m",
    "apparent_temperature",
    "rain",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
]


def _fake_record(hours: int = 2) -> dict:
    return {"hourly": {key: list(range(hours)) for key in HOURLY_KEYS}}


def _frozen_now(day: date):
    """讓模組內的 datetime.now(TAIPEI) 回傳指定日期。"""
    return patch(
        "src.task.e_crawling_weather.datetime",
        **{"now.return_value": datetime(day.year, day.month, day.day, tzinfo=TAIPEI)},
    )


"""========================完成判定（決策三）========================"""


def test_月底當天不算完成因為最後三天還抓不到():
    """核心：用「月已過去」當判準會讓每個月的最後 3 天永久缺失。

    1/31 的 run 只請求得到 1/28，此時一月**不算完成**，
    否則 2/03 之後就再也不會有人回頭補 1/29～1/31。
    """
    assert is_month_complete(2026, 1, date(2026, 1, 31)) is False
    assert month_date_range(2026, 1, date(2026, 1, 31)) == (
        "2026-01-01",
        "2026-01-28",
    )


def test_最後一天進入可抓範圍後才算完成():
    """1/31 要到 2/03（= 1/31 + 3 天延遲）才查得到，屆時一月才算抓完。"""
    assert is_month_complete(2026, 1, date(2026, 2, 2)) is False
    assert is_month_complete(2026, 1, date(2026, 2, 3)) is True
    assert month_date_range(2026, 1, date(2026, 2, 3)) == (
        "2026-01-01",
        "2026-01-31",
    )


def test_歷史年份的每個月都算完成():
    """年份分支已刪除；歷史年份靠同一條規則得出「全部完成」。"""
    today = date(2026, 8, 5)
    assert all(is_month_complete(2025, m, today) for m in range(1, 13))


def test_尚未開始的月份不入列而未來年份得到空清單():
    """空清單是正常結果 —— prep_batch_plan 因此排出空計畫，而不是拋錯。"""
    assert months_to_fetch(2026, date(2026, 8, 5)) == [1, 2, 3, 4, 5, 6, 7, 8]
    assert months_to_fetch(2027, date(2026, 8, 5)) == []


"""========================檔名與路徑（決策一）========================"""


def test_不同月的同號批次不會互相覆蓋():
    """暫存檔路徑不帶月份的話，1 月的第 0 批會被 2 月的第 0 批蓋掉。"""
    assert _batch_object(2026, 1, 0) != _batch_object(2026, 2, 0)


def test_觀測點檔名不含批號():
    """批號隨每次 run 的待抓清單浮動，寫進檔名會讓同一觀測點產生重複檔案。"""
    assert blob_file_name(24.15, 121.55) == "24-15_121-55.parquet"


def test_盤點用的檔名與存檔用的檔名一致():
    """核心：ADR-0013 之前這兩處各自組字串且對不上，續跑的排除從未生效過。

    這裡實際跑一次抓取、取出它寫到 GCS 的物件路徑，再把那些路徑餵回
    prep_batch_plan()，斷言該月已無任何缺口。
    """
    written = []

    with (
        _frozen_now(date(2026, 8, 5)),
        patch(
            "src.task.e_crawling_weather.gcs_utils.read_parquet",
            return_value=DF_TWO_POINTS.copy(),
        ),
        patch(
            "src.task.e_crawling_weather.gcs_utils.write_parquet",
            side_effect=lambda bucket, object_name, df, **kw: written.append(
                object_name
            ),
        ),
        patch(
            "src.task.e_crawling_weather._request_weather_api",
            return_value=[_fake_record(), _fake_record()],
        ),
        patch("src.task.e_crawling_weather.time.sleep"),
    ):
        e_crawler_weatherapi(batch_id=0, target_year=2026, month=1)

    assert len(written) == 2
    assert all(name.startswith(weather_data_prefix(2026, 1) + "/") for name in written)

    # 把剛寫出去的檔案當成 GCS 現況，一月應該再也排不出任何批次
    with (
        _frozen_now(date(2026, 8, 5)),
        patch(
            "src.task.e_crawling_weather.gcs_utils.list_parquet",
            side_effect=lambda bucket, prefix: (
                written if prefix == weather_data_prefix(2026, 1) else []
            ),
        ),
        patch("src.task.e_crawling_weather.gcs_utils.write_parquet"),
    ):
        plan = prep_batch_plan(DF_TWO_POINTS.copy(), 2026, batch_size=50)

    assert not [item for item in plan if item["month"] == 1]


"""========================批次計畫（決策四）========================"""


def test_批次計畫是觀測點與月的笛卡兒積():
    """一個批次 = 一批觀測點 × 一個月，額度因此是算得出來的常數。"""
    with (
        _frozen_now(date(2026, 8, 5)),
        patch("src.task.e_crawling_weather.gcs_utils.list_parquet", return_value=[]),
        patch("src.task.e_crawling_weather.gcs_utils.write_parquet"),
    ):
        plan = prep_batch_plan(DF_TWO_POINTS.copy(), 2026, batch_size=1)

    # 8 個月 × 2 個觀測點 ÷ 每批 1 個 = 16 個批次
    assert len(plan) == 16
    assert {item["month"] for item in plan} == {1, 2, 3, 4, 5, 6, 7, 8}
    assert plan[0] == {"batch_id": 0, "target_year": 2026, "month": 1}


def test_尚未完成的月不排除既有檔案而完成的月會排除():
    """當前月每次 run 整月重抓覆蓋；已完成的月才靠檔名排除。"""
    all_saved = [
        f"{weather_data_prefix(2026, m)}/{blob_file_name(lat, lon)}"
        for m in range(1, 9)
        for lat, lon in zip(DF_TWO_POINTS["lat_round"], DF_TWO_POINTS["lon_round"])
    ]

    with (
        _frozen_now(date(2026, 8, 5)),
        patch(
            "src.task.e_crawling_weather.gcs_utils.list_parquet",
            side_effect=lambda bucket, prefix: [
                name for name in all_saved if name.startswith(prefix + "/")
            ],
        ),
        patch("src.task.e_crawling_weather.gcs_utils.write_parquet"),
    ):
        plan = prep_batch_plan(DF_TWO_POINTS.copy(), 2026, batch_size=50)

    # 1～7 月已完成且檔案齊全 -> 排除；8 月尚未完成 -> 仍要重抓
    assert [item["month"] for item in plan] == [8]


def test_全部抓完時回傳空清單而不是拋錯():
    """d08 補完之後每天都會走到這裡，那是正常結果，不是故障。"""
    all_saved = [
        f"{weather_data_prefix(2025, m)}/{blob_file_name(lat, lon)}"
        for m in range(1, 13)
        for lat, lon in zip(DF_TWO_POINTS["lat_round"], DF_TWO_POINTS["lon_round"])
    ]

    with (
        _frozen_now(date(2026, 8, 5)),
        patch(
            "src.task.e_crawling_weather.gcs_utils.list_parquet",
            side_effect=lambda bucket, prefix: [
                name for name in all_saved if name.startswith(prefix + "/")
            ],
        ),
        patch("src.task.e_crawling_weather.gcs_utils.write_parquet"),
    ):
        assert prep_batch_plan(DF_TWO_POINTS.copy(), 2025, batch_size=50) == []
