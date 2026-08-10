# 執行摘要：事故主鍵與 upsert 範疇

- 日期：2026-08-09
- 分支：`refactor/util-connection-layer`
- 依據：[ADR-0014 事故主鍵由事故內容決定，而非單次 run 的排序名次](./0014-事故主鍵由事故內容決定而非單次run排序名次.md)
- 前一輪：無
- 性質：**interim execution report**

| 候選 | 標題 | 狀態 |
| --- | --- | --- |
| 候選 14 | 事故主鍵與 upsert 範疇 | 程式碼已完成；三張事實表待重灌 |

本候選啟發於 2026-08-09 d02 實際失敗引發的排查：
`task_t_and_l_other_facts` 寫入 `fact_accident_human` 時拋出 IntegrityError
（`accident_id` 為 NULL）。

---

## 一、病因定義

`fact_accident_main` 的主鍵是「這次 run 的輸入裡排第幾」，不是事故的身分。

```python
# t_fact_accident_main.py（修改前）
df_fact_accident_main = df_fact_accident_main.sort_values(
    by=["day_id", "accident_time", "longitude", "latitude"]
).reset_index(drop=True)
df_fact_accident_main["cumcount"] = (
    df_fact_accident_main.groupby("accident_date").cumcount() + 1
)
```

來源在 2026-08-08 更新載點、對 7 月補登資料（**實測**：使用者查看 data.gov.tw
資料集頁面）。補登的事故排序後插在某日中段，該日其後每一件的名次全部 +1，
於是新算出的編號撞到 DB 裡**別件事故**已佔用的主鍵。而
`l_fact_accident_main` 的 `update_columns=["accident_time"]` 讓這個衝突
不是插入、而是把那一列的時間蓋成新事故的時間，座標留在原地：

```
run 1:  D-0001=(05:00, X)  D-0002=(08:00, Y)  D-0003=(09:00, Z)
        ↓ 補登一筆 (04:00, W)
run 2:  D-0001=(04:00, X)  D-0002=(05:00, Y)  D-0003=(08:00, Z)  D-0004=(09:00, Z)
              ↑ 時間來自前一名，座標沒跟著改，所以這四欄組合在來源 CSV 檔不存在
```

下游 `t_fact_accident_human` 用唯一鍵四欄回查 `accident_id`，對這些列全部落空。

**病因的一般形式：識別一個實體的鍵，取決於執行脈絡而不是實體本身。**

### 排查中走的三段冤路（各花一輪往返）

三個檢查都只看 `fact_accident_main` 自己，而平移錯位在表內部完全自洽：

| 檢查 | 實際結果 | 為何無效 |
| --- | --- | --- |
| `LEFT(accident_id,8) <> 日期` | 0 | 前綴就是日期，撞號只發生在同日內部 |
| 每日 `COUNT(*) <> MAX(序號)` | 空 | 撞號從第 k 名起密集連續，末尾照常 INSERT |
| 同日 `accident_id` 遞增而 `accident_time` 遞減 | 空 | 錯位是整段平移，時間序列仍然遞增 |

定案靠的是使用者手動比對：`2026070200000030` 的時間是 `06:47:00`（補登那筆的），
座標卻是原 `06:53:00` 那件的（**實測**）。**唯一有效的判準是拿表裡的唯一鍵四欄
去跟來源 CSV 比對**，不參照來源的檢查都偵測不到。

## 二、決策摘要

| # | 決策 | 一句話理由 |
| --- | --- | --- |
| 一 | `accident_id` = 日期八碼 + 唯一鍵四欄的 SHA-256 前 16 碼，`VARCHAR(24)` | 主鍵只由事故本身決定，補登不會讓任何既有事故換號 |
| — | `sort_values` 一併移除 | 它只為流水號存在；留著會讓人以為順序仍有意義，而那正是病灶 |
| 二 | `update_columns` 只列非鍵欄位，排除 `weather_record_id` | 鍵欄位進 update 範疇就是製造嵌合列；`weather_record_id` 不由 d02 產生 |
| 三 | 兩支 `t_` 對不到主檔一律 raise，`how="left"` 維持不變 | 故障要在說得出原因的地方停下（ADR-0003） |
| 四 | 三張事實表重灌 | 已落地的嵌合列不會因重跑而修正，且舊編號全部作廢 |

被排除的四個方案（純雜湊不加前綴、8 碼雜湊、流水號續編、複合主鍵）與
「只補齊 `update_columns` 不動主鍵」的排除理由，見 ADR 決策一與二。

## 三、實際改動

三個 commit，213 個測試全綠：

| Commit | 內容 |
| --- | --- |
| `2ae009d` | `docs:initiate ADR-0014 for content-derived accident primary key` |
| `34c35a6` | `fix:revise accident primary key to derive from content instead of row order` |
| `e7540c8` | `feat:initiate guard against unmatched accident id in fact human and env` |

| 檔案 | 改動 |
| --- | --- |
| `t_fact_accident_main.py` | `prefix`/`cumcount` 換成日期前綴 + SHA-256；經緯度先 `f"{v:.6f}"` 再入雜湊；移除 `sort_values`，`drop_duplicates` 後直接 `reset_index` |
| `create_traffic_accident_tables.py` | 三張表的 `accident_id` 由 `VARCHAR(16)` 改 `VARCHAR(24)` |
| `l_fact_accident_main.py` | `update_columns` 改 `["accident_type_id", "death_count", "injury_count"]` |
| `t_fact_accident_human.py`、`t_fact_accident_env.py` | 回查後 `accident_id` 有 NULL 就 `raise ValueError`，訊息帶筆數與前 5 組對不上的鍵 |
| `test_task_t_fact_main_pk_stability.py`（新增） | 3 個測試 |
| `test_task_loaders.py`、`test_util_read_traffic_accident_file.py` | 兩處釘住舊行為的斷言改成釘新契約 |

**偏離 ADR 之處：無。** `sort_values` 的移除在 ADR commit 前已折進決策一，
不是實作期的偏離。

## 四、驗證方式

已完成：

- 213 個測試全綠（本輪新增 3 個）。新測試釘的是**既有事故的編號不受同時讀進來的
  其他資料影響**，中段補登一筆後重跑，原有三件的編號一個都不能變。
  這正是舊實作會失敗的那個情境。
- `pre-commit` 對所有改動檔案通過（ruff check / format、trailing whitespace 等）。
- 離線以真實 CSV 重跑兩支 `t_`（A1 即時檔 + A2 第 4 分檔，71,821 列 / 31,666 件），
  同一次 run 的輸入內 NULL `accident_id` 為 0，證實清洗邏輯本身對得上，
  病灶只在跨 run 時出現。

**尚未驗證（需要真實 DB，重灌後才能做）：**

1. main 的唯一鍵四欄集合與來源 CSV 清洗後的四欄集合 anti-join 兩邊皆空。
2. 同一份 CSV 連跑兩次 d02，列數與每列 `accident_id` 完全不變。
3. `task_t_and_l_other_facts` 不再出現 NULL `accident_id`。

## 五、同病因但尚未修復的位置

病因的一般形式是「鍵取決於執行脈絡」與「update 範疇沒有依鍵／非鍵決定」。
以下是 `grep` 全部 `l_*.py` 的實測結果：

### 1. 四支 loader 把唯一鍵的成員放進 `update_columns`

| Loader | `update_columns` 中屬於鍵的欄位 | 該表的 UNIQUE KEY |
| --- | --- | --- |
| `l_dim_accident_type` | `accident_category` | 五欄，含 `accident_category` |
| `l_dim_lane_design` | `lane_edge_marking` | 五欄，含 `lane_edge_marking` |
| `l_dim_road_design` | `road_form_minor` | 三欄，含 `road_form_minor` |
| `l_fact_hourly_weather` | `observation_datetime`、`longitude_round`、`latitude_round`（另有七個非鍵欄位） | `hash_value`，**正是由這三欄雜湊而成**（使用者確認） |

與 ADR-0014 決策二禁止的是同一件事。**但四支目前都不會出錯**，理由相同：
唯一鍵涵蓋了那些欄位（三張 dim 的 UNIQUE 就是全部業務欄位；`hash_value` 是那
三欄的 SHA-256），所以衝突就代表那些欄位的值必然相同，更新成同值無害。

這是「語意錯誤但後果為零」，不是缺陷，列在這裡是因為它顯示這個原則
從未被明確過。

### 2. `update_columns` 的範疇沒有一致原則

`l_fact_accident_env` 只更新 `weather_condition` 一欄、`l_fact_accident_human`
只更新 `hit_and_run` 一欄，其餘十幾個非鍵欄位在衝突時不會被更新，
來源若修正了 `light_condition` 或 `age`，重跑不會反映。這與 main 原本只列
`accident_time` 是同一種「範疇太窄」。

各 loader 為何挑那一欄，程式碼裡沒有任何說明（**推論**：沒有原則，是隨手挑的）。

反例是 `l_fact_hourly_weather` 是唯一一支以「hash_value 是唯一鍵，
衝突時更新其餘欄位」為原則做 upsert 的腳本。

### 3. `fact_accident_human` 的 `row_hash` 唯一性從未被斷言

`t_fact_accident_human` 算完 `row_hash` 後只把重複筆數記進 log
（「留著檢查 row_hash 設計是否足夠確保業務唯一性」），沒有任何斷言。
`row_hash` 是 `UNIQUE NOT NULL`，重複代表兩個當事人會被折成一列，不確定是否真的會發生，長期觀察。

## 六、現況盤點

**程式碼與資料庫現在是不一致的，重灌前不能跑 d02／d03。**

| 項目 | 狀態 |
| --- | --- |
| 程式碼 | 已產出 24 碼 `accident_id` |
| DB 綱要 | 仍是 `VARCHAR(16)` |
| 後果 | strict mode 下報 error 1406；非 strict 會**靜默截斷**（**推論**，未實測本專案設定） |

其餘現況：

- `fact_accident_main` 仍含嵌合列，數量**未測**（已知至少 1 筆）。
  已知受影響區間是 `day_id` 2009–2024（＝2026 年 7 月，**實測**自 Airflow log）。
- 三張事實表的資料仍是舊編號體系；四張 `dim_*` 不受影響。

## 七、建議的下一步

1. **重灌三張事實表**（必做）。
   `DROP TABLE fact_accident_human, fact_accident_env, fact_accident_main;`
   （子表先走才不會卡外鍵）→ `create_traffic_accident_tables` 建新綱要 →
   跑 d02（今年）與 d03（歷年約 390 萬列）→ 照第四章的三項驗證確認。
   四張 `dim_*` 不要動。
2. **把 `update_columns` 的原則推廣到其餘 loader** (建議不強制)。
   原則已由 ADR-0014 決策二確立、`l_fact_hourly_weather` 的雜湊做法已有先例，
   缺的是套到另外六支並補上說明。
3. **決定 `row_hash` 的唯一性要不要斷言** (留作觀察)。它與本輪同屬
   「鍵的設計」，但需要先看真實資料的重複筆數再行動。
