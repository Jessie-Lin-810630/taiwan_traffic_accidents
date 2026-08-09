"""驗證事故 CSV 的單一讀取契約，以及六支 t_*.py 對它的使用。

依 ADR-0010，這份契約要釘住的是「失敗語意」而不是「不重複」：

- 缺欄時按名稱對應，缺席的欄成為 NaN 且不讓其後的欄位錯位
  （舊實作按位置賦名，59/72 個真實檔案因此錯位六欄）
- 缺欄要留下 warning，但不中止 —— 來源會演進，缺欄不是故障
- 空 pathlist 六支一律 raise —— 那是上游沒抓到檔案，不是「查無資料」
"""

import importlib
from unittest.mock import patch

import pandas as pd
import pytest

from src.util.read_traffic_accident_file import read_traffic_accident_file
from src.util.table_column_map import (
    dim_accident_type_col_map,
    dim_lane_design_col_map,
    dim_road_design_col_map,
    fact_accident_env_col_origin_map,
    fact_accident_human_col_origin_map,
    fact_accident_main_col_origin_map,
)

# 六支 t_*.py 與它們各自的欄位對照表
TRANSFORMS = [
    ("t_dim_accident_type", dim_accident_type_col_map),
    ("t_dim_lane_design", dim_lane_design_col_map),
    ("t_dim_road_design", dim_road_design_col_map),
    ("t_fact_accident_main", fact_accident_main_col_origin_map),
    ("t_fact_accident_env", fact_accident_env_col_origin_map),
    ("t_fact_accident_human", fact_accident_human_col_origin_map),
]

# 舊表頭（111～113 年度）缺少的那一欄，114 年度才新增
COLUMN_ADDED_IN_2025 = "共享經濟或外送平台的名稱"


def _write_csv(tmp_path, columns, rows, name="accident.csv"):
    """寫出一個帶兩行統計備註的事故 CSV，模擬 data.gov.tw 的檔案格式。"""
    footer = [{c: "備註" for c in columns}, {c: "備註" for c in columns}]
    path = tmp_path / name
    pd.DataFrame(rows + footer).to_csv(path, index=False, encoding="utf-8")
    return path


def _self_describing_rows(columns, column_map, n=1):
    """每一格填入它「應該」對應到的英文欄名，錯位就能一眼看出。"""
    return [{c: f"<{column_map[c]}>" for c in columns} for _ in range(n)]


class Test讀取契約:
    """read_traffic_accident_file 這個 interface 本身的行為。"""

    def test_缺欄時其後的欄位不會錯位(self, tmp_path):
        """舊表頭少一欄時，其餘 20 欄的值必須仍與自己的欄名相符。

        這是 ADR-0010 的核心：舊實作把補上的欄放在最後、卻按原順序賦名，
        導致缺席欄之後的每一欄都往前挪一格。
        """
        column_map = fact_accident_human_col_origin_map
        columns = [c for c in column_map if c != COLUMN_ADDED_IN_2025]

        path = _write_csv(tmp_path, columns, _self_describing_rows(columns, column_map))
        df = read_traffic_accident_file(path, column_map)

        misplaced = [
            (name, value)
            for name, value in zip(df.columns, df.iloc[0])
            if pd.notna(value) and value != f"<{name}>"
        ]
        assert misplaced == []

    def test_缺欄的那一欄是_NaN_而不是別人的值(self, tmp_path):
        """缺席的欄位要留在原位並成為 NaN，不得裝著鄰欄的值。"""
        column_map = fact_accident_human_col_origin_map
        columns = [c for c in column_map if c != COLUMN_ADDED_IN_2025]

        path = _write_csv(tmp_path, columns, _self_describing_rows(columns, column_map))
        df = read_traffic_accident_file(path, column_map)

        assert df["serving_sharing_economy_or_delivery"].isna().all()

    def test_欄位順序永遠與對照表一致(self, tmp_path):
        """Reindex 保證輸出欄序等於對照表，與 CSV 實際欄序無關。"""
        column_map = fact_accident_human_col_origin_map
        columns = [c for c in column_map if c != COLUMN_ADDED_IN_2025]

        path = _write_csv(tmp_path, columns, _self_describing_rows(columns, column_map))
        df = read_traffic_accident_file(path, column_map)

        assert list(df.columns) == list(column_map.values())

    def test_缺欄時留下_warning(self, tmp_path, caplog):
        """缺欄不中止，但不能無聲無息。"""
        column_map = fact_accident_human_col_origin_map
        columns = [c for c in column_map if c != COLUMN_ADDED_IN_2025]

        path = _write_csv(tmp_path, columns, _self_describing_rows(columns, column_map))
        with caplog.at_level("WARNING"):
            read_traffic_accident_file(path, column_map)

        assert COLUMN_ADDED_IN_2025 in caplog.text

    def test_欄位齊全時不發_warning(self, tmp_path, caplog):
        """新表頭沒有缺欄，不該產生噪音。"""
        column_map = fact_accident_human_col_origin_map
        columns = list(column_map)

        path = _write_csv(tmp_path, columns, _self_describing_rows(columns, column_map))
        with caplog.at_level("WARNING"):
            read_traffic_accident_file(path, column_map)

        assert caplog.text == ""

    def test_末兩行統計備註被_skipfooter_丟掉(self, tmp_path):
        """skipfooter=2 是對 data.gov.tw 檔案格式的斷言，只有這一處。"""
        column_map = dim_road_design_col_map
        columns = list(column_map)

        path = _write_csv(
            tmp_path, columns, _self_describing_rows(columns, column_map, n=3)
        )
        df = read_traffic_accident_file(path, column_map)

        assert len(df) == 3
        assert "備註" not in df.to_string()

    def test_字串欄位的前後空白被去掉(self, tmp_path):
        """去空白從六支的迴圈末尾移進讀取契約，行為必須保留。"""
        column_map = dim_road_design_col_map
        columns = list(column_map)
        rows = [{c: f"  {column_map[c]}  " for c in columns}]

        path = _write_csv(tmp_path, columns, rows)
        df = read_traffic_accident_file(path, column_map)

        assert list(df.iloc[0]) == list(column_map.values())

    def test_對照表以外的欄位不會被帶進來(self, tmp_path):
        """Reindex 同時負責挑欄，多餘的欄位不得外流。"""
        column_map = dim_road_design_col_map
        columns = list(column_map) + ["發生年度", "發生月份"]
        rows = [{c: "x" for c in columns}]

        path = _write_csv(tmp_path, columns, rows)
        df = read_traffic_accident_file(path, column_map)

        assert list(df.columns) == list(column_map.values())


class Test空輸入:
    """六支 t_*.py 對空 pathlist 的失敗語意。"""

    @pytest.mark.parametrize("module_name, _", TRANSFORMS)
    def test_空_pathlist_六支一律_raise(self, module_name, _):
        """空 pathlist 代表上游沒抓到檔案，不得回空 DataFrame（ADR-0003）。"""
        module = importlib.import_module(f"src.task.{module_name}")
        transform = getattr(module, module_name)

        with pytest.raises(ValueError, match="上游未產出任何 CSV 檔"):
            transform([])


class Test維度表端到端:
    """dim 三支不碰 MySQL，可以完整跑完。"""

    @pytest.mark.parametrize(
        "module_name, column_map",
        [
            ("t_dim_accident_type", dim_accident_type_col_map),
            ("t_dim_lane_design", dim_lane_design_col_map),
            ("t_dim_road_design", dim_road_design_col_map),
        ],
    )
    def test_跨檔去重且欄名正確(self, module_name, column_map, tmp_path):
        """Dim 三支的既有行為（逐檔去重 + 跨檔去重 + reset_index）不因抽出而改變。"""
        columns = list(column_map)
        # 兩個檔案，各自有重複列，且彼此重複
        rows = _self_describing_rows(columns, column_map, n=2)
        path_a = _write_csv(tmp_path, columns, rows, name="a.csv")
        path_b = _write_csv(tmp_path, columns, rows, name="b.csv")

        module = importlib.import_module(f"src.task.{module_name}")
        df = getattr(module, module_name)([str(path_a), str(path_b)])

        assert list(df.columns) == list(column_map.values())
        assert len(df) == 1
        assert list(df.index) == [0]


class Test事實表端到端:
    """fact 三支中段會查 MySQL 取維度，這裡把那道查詢換成假的維度表。

    mock 出來的維度表形狀是人工假設 —— 這筆債屬於下一個候選
    （transform 內嵌的維度查閱），見 ADR-0010「誠實的限制」。
    """

    ACCIDENT_ROW = {
        "發生日期": "20240101",
        "發生時間": "83000",
        "經度": "121.5",
        "緯度": "25.0",
    }

    def _accident_csv(self, tmp_path, column_map, extra=None):
        # 維度鍵一律用非數字的值，避免 pandas 把它推成 int64 而無法與維度表 merge
        columns = list(column_map)
        row = {}
        for c in columns:
            row[c] = self.ACCIDENT_ROW.get(c, (extra or {}).get(c, "甲"))
        return _write_csv(tmp_path, columns, [row])

    def test_main_的日期時間與死傷人數(self, tmp_path):
        """讀取契約換掉後，main 自己的清洗與流水號生成仍然正確。"""
        path = self._accident_csv(
            tmp_path,
            fact_accident_main_col_origin_map,
            extra={"死亡受傷人數": "死亡0;受傷2"},
        )

        dim_day = pd.DataFrame({"day_id": [1], "accident_date": ["2024-01-01"]})
        dim_type = pd.DataFrame(
            {
                "accident_type_id": [1],
                **{
                    v: ["甲"]
                    for k, v in fact_accident_main_col_origin_map.items()
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

        with patch(
            "src.task.t_fact_accident_main.get_table_from_sqlserver",
            side_effect=[dim_day, dim_type],
        ):
            from src.task.t_fact_accident_main import t_fact_accident_main

            df = t_fact_accident_main([str(path)])

        assert len(df) == 1
        row = df.iloc[0]
        assert row["accident_time"] == "08:30:00"
        assert row["death_count"] == 0
        assert row["injury_count"] == 2
        # 前 8 碼是日期、其後 16 碼是唯一鍵四欄的雜湊，參考 ADR-0014
        assert row["accident_id"].startswith("20240101")
        assert len(row["accident_id"]) == 24

    def test_human_的肇逃欄拿到真實值而不是恆為零(self, tmp_path):
        """舊表頭缺的是共享經濟欄，肇逃欄本來就在 —— 錯位修好後它必須是真值。"""
        column_map = fact_accident_human_col_origin_map
        columns = [c for c in column_map if c != COLUMN_ADDED_IN_2025]
        row = {c: self.ACCIDENT_ROW.get(c, "1") for c in columns}
        row["肇事逃逸類別名稱-是否肇逃"] = "是"
        row["當事者屬-性-別名稱"] = "男"
        row["當事者順位"] = "1"
        row["車輛撞擊部位子類別名稱-其他"] = "左側車身"
        path = _write_csv(tmp_path, columns, [row])

        dim_day = pd.DataFrame({"day_id": [1], "accident_date": ["2024-01-01"]})
        fact_main = pd.DataFrame(
            {
                "accident_id": ["2024010100000001"],
                "day_id": [1],
                "accident_time": ["08:30:00"],
                "longitude": [121.5],
                "latitude": [25.0],
            }
        )

        with patch(
            "src.task.t_fact_accident_human.get_table_from_sqlserver",
            side_effect=[dim_day, fact_main],
        ):
            from src.task.t_fact_accident_human import t_fact_accident_human

            df = t_fact_accident_human([str(path)])

        row_out = df.iloc[0]
        assert row_out["hit_and_run"] == 1
        assert row_out["impact_point_minor_other"] == "左側車身"
        assert row_out["serving_sharing_economy_or_delivery"] is None

    def test_env_的欄位不會錯位(self, tmp_path):
        """Env 的四次維度 merge 之後，欄位仍與欄名相符。"""
        column_map = fact_accident_env_col_origin_map
        columns = list(column_map)
        row = {c: self.ACCIDENT_ROW.get(c, "甲") for c in columns}
        row["速限-第1當事者"] = "50"
        row["天候名稱"] = "晴"
        path = _write_csv(tmp_path, columns, [row])

        dim_day = pd.DataFrame({"day_id": [1], "accident_date": ["2024-01-01"]})
        dim_road = pd.DataFrame(
            {
                "road_design_id": [1],
                "road_type_primary_party": ["甲"],
                "road_form_major": ["甲"],
                "road_form_minor": ["甲"],
            }
        )
        dim_lane = pd.DataFrame(
            {
                "lane_design_id": [1],
                "lane_divider_direction_major": ["甲"],
                "lane_divider_direction_minor": ["甲"],
                "lane_divider_main_general": ["甲"],
                "lane_divider_fast_slow": ["甲"],
                "lane_edge_marking": ["甲"],
            }
        )
        fact_main = pd.DataFrame(
            {
                "accident_id": ["2024010100000001"],
                "day_id": [1],
                "accident_time": ["08:30:00"],
                "longitude": [121.5],
                "latitude": [25.0],
            }
        )

        with patch(
            "src.task.t_fact_accident_env.get_table_from_sqlserver",
            side_effect=[dim_day, dim_road, dim_lane, fact_main],
        ):
            from src.task.t_fact_accident_env import t_fact_accident_env

            df = t_fact_accident_env([str(path)])

        row_out = df.iloc[0]
        assert row_out["weather_condition"] == "晴"
        assert row_out["speed_limit_primary_party"] == 50
        assert row_out["accident_id"] == "2024010100000001"
