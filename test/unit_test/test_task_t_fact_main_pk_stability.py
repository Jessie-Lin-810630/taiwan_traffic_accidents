"""驗證 `fact_accident_main` 的主鍵由事故內容決定，不隨每次 run 讀到的資料浮動。

釘住的是 ADR-0014。舊實作把主鍵定成「排序後當日的第幾件」，於是來源在既有
日期補登一筆事故時，該日之後每一件都換號 —— 換到的號碼是別件事故已佔用的
主鍵，upsert 因此把 INSERT 變成 UPDATE，只蓋掉 `accident_time` 而留下座標，
產生「時間來自 A、座標來自 B」的列，下游 `t_fact_accident_human` 便回查不到
`accident_id`。

因此這裡要釘的不是「編號長什麼樣」，而是**既有事故的編號不受同時讀進來的其他資料影響**。
"""

from unittest.mock import patch

import pandas as pd

from src.util.table_column_map import fact_accident_main_col_origin_map

DIM_DAY = pd.DataFrame({"day_id": [1], "accident_date": ["2024-01-01"]})
DIM_TYPE = pd.DataFrame(
    {
        "accident_type_id": [1],
        **{
            v: ["甲"]
            for v in fact_accident_main_col_origin_map.values()
            if v
            in (
                "accident_category",
                "accident_position_major",
                "accident_position_minor",
                "accident_type_major",
                "accident_type_minor",
            )
        },
    }
)


def _csv_of_accidents(tmp_path, times, name):
    """寫出同一天、數個時間點各一件事故的 CSV，附兩行統計備註。"""
    columns = list(fact_accident_main_col_origin_map)
    rows = []
    for t in times:
        row = dict.fromkeys(columns, "甲")
        row["發生日期"] = "20240101"
        row["發生時間"] = t
        row["經度"] = "121.5"
        row["緯度"] = "25.0"
        row["死亡受傷人數"] = "死亡0;受傷1"
        rows.append(row)
    footer = [dict.fromkeys(columns, "備註")] * 2
    path = tmp_path / name
    pd.DataFrame(rows + footer).to_csv(path, index=False, encoding="utf-8")
    return path


def _run(path):
    with patch(
        "src.task.t_fact_accident_main.get_table_from_sqlserver",
        side_effect=[DIM_DAY.copy(), DIM_TYPE.copy()],
    ):
        from src.task.t_fact_accident_main import t_fact_accident_main

        df = t_fact_accident_main([str(path)])
    return dict(zip(df["accident_time"], df["accident_id"]))


def test_補登一筆事故不會讓其他事故換號(tmp_path):
    """ADR-0014 的核心：主鍵是事故內容的函數，不是它在這次輸入裡的排序名次。

    舊實作下，插在最前面的 05:30 會讓其餘三件全部換號並撞上彼此的主鍵。
    """
    before = _run(_csv_of_accidents(tmp_path, ["083000", "090000", "100000"], "a.csv"))
    after = _run(
        _csv_of_accidents(tmp_path, ["053000", "083000", "090000", "100000"], "b.csv")
    )

    assert len(before) == 3
    assert len(after) == 4
    for time, accident_id in before.items():
        assert after[time] == accident_id, f"{time} 的編號因為補登而改變了"


def test_同一份輸入重跑得到同一組編號(tmp_path):
    """Upsert 要冪等，前提是同一件事故每次都算出同一個主鍵。"""
    path = _csv_of_accidents(tmp_path, ["083000", "090000"], "c.csv")

    assert _run(path) == _run(path)


def test_主鍵前八碼是日期且總長二十四碼(tmp_path):
    """欄位寬度 VARCHAR(24) 與「前綴可當日期用」兩件事互為約束。"""
    result = _run(_csv_of_accidents(tmp_path, ["083000"], "d.csv"))

    accident_id = result["08:30:00"]
    assert accident_id.startswith("20240101")
    assert len(accident_id) == 24
