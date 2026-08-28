"""Streamlit 分頁：全臺夜市事故嚴重度分析。

讀取 DAG 預先算好的全臺總表（Redis 鍵 `mart:pedestrian_national_master`），依地區、
縣市與時間篩選後繪出各項圖表。本頁不做重運算，也不直接查 MySQL。
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from redis.exceptions import RedisError

import src.task.core.c_ui as ui
from src.util.logger_crtx import get_logger
from src.util.redis_utils import get_cache

logger = get_logger(__name__)

st.set_page_config(layout="wide", page_title="全台夜市事故嚴重分析", page_icon="🚦")


def get_region_or_cities(
    *, city: str | None = None, region: str | None = None
) -> str | list | None:
    """由縣市反查所屬地區，或由地區取得其縣市清單，兩者只能擇一傳入。

    地區分成北部、中部、南部、東部與東部離島、其他離島五類。

    Args:
        city (str | None): 縣市名稱，例如 `"臺北市"`。
        region (str | None): 地區名稱，例如 `"北部"`。

    Returns:
        str | list | None: 傳入縣市時回傳所屬地區名稱，查不到時回傳
            `"找不到地區"`；傳入地區時回傳縣市清單，形如
            `["臺北市", "新北市", "基隆市", "桃園市", "新竹市", "新竹縣"]`，
            查不到時回傳空字串；兩者都傳入時回傳提示字串；都不傳則為 `None`。
    """
    # 五個地區的定義見 ADR-0020，全專案共用同一組字面值
    cities_per_region = {
        "北部": ["臺北市", "新北市", "基隆市", "桃園市", "新竹市", "新竹縣", "宜蘭縣"],
        "中部": ["臺中市", "彰化縣", "南投縣", "雲林縣", "苗栗縣"],
        "南部": ["臺南市", "高雄市", "屏東縣", "嘉義市", "嘉義縣"],
        "東部與東部離島": ["花蓮縣", "臺東縣"],
        "其他離島": ["澎湖縣", "金門縣", "連江縣"],
    }
    if city and region:
        return "只能傳入city或region任一！"

    if city:
        for a_region, cities in cities_per_region.items():
            if city in cities:
                return a_region
        return "找不到地區"
    elif region:
        return cities_per_region.get(region, "")
    else:
        return None


# 效能優化：Streamlit 本地快取
# 使用 @st.cache_data 將讀取到的 DataFrame 暫存在伺服器記憶體中
# 這樣使用者在切換篩選條件時，就不必重新去 Redis 撈取幾十 MB 的大表
@st.cache_data(ttl=36000, show_spinner=False)
def get_dynamic_national_data() -> pd.DataFrame:
    """讀取預計算的全臺總表，並剔除跨夜市重疊的重複事故。

    臺北市等密集區域會有多個夜市的範圍相互重疊，同一件事故因此被算進多個夜市，
    這裡依 `accident_id` 去重以確保全臺總數正確。結果另以 Streamlit 的快取存在
    伺服器記憶體 10 小時，使用者切換篩選條件時就不必重新從 Redis 取回大表。

    Returns:
        pandas.DataFrame: 去重後的全臺總表，形如：

            accident_id      accident_date  nightmarket_name  nightmarket_city  death_count  injury_count  pdi_score
            2024010100000001  2024-01-01     士林夜市          臺北市            0            1             3.0
            2024010100000002  2024-01-01     逢甲夜市          臺中市            1            0             15.0

            快取不存在、為空或缺少 `accident_id` 欄位時回傳空 DataFrame。

    Raises:
        RedisError: 讀取快取失敗。
    """
    cache_key = "mart:pedestrian_national_master"  # 理應在dags/d06_precompute_to_redis.py存入Redis
    unpickled_data = get_cache(cache_key)
    # unpickled_data = pd.DataFrame(unpickled_data)
    if isinstance(unpickled_data, pd.DataFrame) and not unpickled_data.empty:
        # 確保跨夜市 500公尺重疊區域的事故不被重複計算
        # 去重複機制
        # 因為台北市等密集區，多個夜市的 500m 範圍會重疊。此處利用 accident_id 剔除重複計算的事故，確保「全台總數」的準確性
        if "accident_id" in unpickled_data.columns:
            df = unpickled_data.drop_duplicates(subset=["accident_id"])
            return df
    return pd.DataFrame()


def main() -> None:
    """組出分頁版面，載入全臺總表並依使用者的篩選繪圖。

    流程是：取得夜市主檔並畫出側邊欄 → 讀取預計算的全臺總表 → 剔除縣市欄為空的
    列 → 左側提供分析視角（綜合危險指數或事故件數）與地區、縣市篩選器，右側
    顯示核心指標與各項圖表。

    兩種失敗各自處理：資料服務層或快取服務故障時記錄並中止渲染；快取只是還沒被
    預計算填上（總表為空）則顯示提示，請使用者確認排程是否跑完。

    Notes:
        「快取故障」與「快取裡沒有這筆資料」的語意分離，參考 ADR-0003。
    """
    ui.render_sidebar()
    ui.load_custom_css()

    st.markdown(
        """
                <h1 style="margin-bottom:5px;">🚦 臺灣夜市交通安全總體檢：<span class="title-highlight">全台數據揭密</span></h1>
                """,
        unsafe_allow_html=True,
    )

    st.info("""
            💡 **數據判讀須知**：
            1. **空間範圍**：本頁指標與排名【包含夜市方圓 500 公尺核心區內所有類型車禍】，並已剔除重疊事故紀錄。
            2. **時間完整性**：⚠️ **2026 Q3 數據目前僅統計至 7 月底**。季度比較趨勢之落差係因資料統計週期不完整所致，非實際事故量大幅下降，判讀時請留意。
            """)

    st.markdown(
        "<hr style='margin-top: 5px; margin-bottom: 20px;'>", unsafe_allow_html=True
    )

    with st.spinner("正在載入全台數據..."):
        try:
            df_raw = get_dynamic_national_data()
        except RedisError:
            logger.error("全台總表快取讀取失敗", exc_info=True)
            st.error("⛔ 快取服務暫時無法使用，請稍後再試或聯繫維運人員。")
            st.stop()

    if df_raw.empty:
        # aggregate_national_master 寫在 dags/d06_precompute_to_redis.py
        st.warning(
            "⚠️ 無法取得全台總表，請確認 Airflow 的 `aggregate_national_master` 排程是否已執行完成。"
        )
        return None

    df_raw = df_raw.dropna(subset=["nightmarket_city"])
    df_raw = df_raw[df_raw["nightmarket_city"].astype(str).str.strip() != "None"]
    df_raw = df_raw[df_raw["nightmarket_city"].astype(str).str.strip() != ""]

    # 左側放篩選器，右側放核心數據指標與熱力圖
    col_left, col_right = st.columns([0.7, 2.3], gap="large")

    with col_left:
        st.markdown(
            '<div class="section-title" style="margin-bottom:8px;">切換分析視角</div>',
            unsafe_allow_html=True,
        )

        # 彈性切換分析指標
        # 讓使用者可以一鍵切換整頁的計算指標，依照選擇，下方的變數 (metric_col, agg_func) 自動配合變動
        options = ["綜合危險指數 (PDI)", "事故總件數"]
        mode = st.radio(
            "切換分析視角", options, horizontal=True, label_visibility="collapsed"
        )

        # 描述連動邏輯
        if mode == options[0]:
            metric_col, agg_func, unit, fmt, sort_col = (
                "pdi_score",
                "mean",
                "分",
                ",.2f",
                "PDI平均",
            )
        elif mode == options[1]:
            metric_col, agg_func, unit, fmt, sort_col = (
                "accident_id",
                "count",
                "件",
                ",.0f",
                "事故總數",
            )

        # 確認使用者是否點選了要分析PDI指標，回傳boolean
        is_pdi_mode = mode == options[0]

        # PDI 說明提醒
        st.markdown(
            """
        <div style="background-color: #f8fafc; border-left: 4px solid #94a3b8; padding: 16px; border-radius: 0 8px 8px 0; margin-bottom: 25px; margin-top: 10px;">
            <div style="font-weight: bold; color: #475569; margin-bottom: 8px; font-size: 14px;">💡 什麼是 PDI 危險指數？</div>
            <ul style="font-size: 13px; color: #475569; line-height: 1.7; margin-bottom: 0; padding-left: 20px;">
                <li><b>單件 PDI：</b> (死亡×10 + 受傷×2) × 時段權重（17 時起至凌晨為 1.5，其餘為 1.0）</li>
                <li><b>本頁數值：</b> 該範圍內所有事故的單件 PDI 平均值，以「每一件事故」為單位標準化，消除規模誤差。</li>
                <li><b>判定：</b> 數值越高代表一旦發生事故「非死即傷」機率越高。</li>
            </ul>
        </div>
        """,
            unsafe_allow_html=True,
        )

        # 時間篩選清單
        st.markdown(
            '<div class="section-title">📅 篩選分析時間</div>', unsafe_allow_html=True
        )
        week_map = {
            0: "全部",
            1: "週一",
            2: "週二",
            3: "週三",
            4: "週四",
            5: "週五",
            6: "週六",
            7: "週日",
        }
        with st.container(border=True):
            l_c1, l_c2 = st.columns(2)
            with l_c1:
                sel_year = st.selectbox(
                    "年份", ["全部年份", "2026", "2025", "2024", "2023", "2022", "2021"]
                )
            with l_c2:
                sel_q = st.selectbox(
                    "季度", ["全年", "第 1 季", "第 2 季", "第 3 季", "第 4 季"]
                )

            l_c3, l_c4 = st.columns(2)
            with l_c3:
                sel_m = st.selectbox(
                    "月份", ["全部"] + [f"{i} 月" for i in range(1, 13)]
                )

            with l_c4:
                sel_w = st.selectbox("星期", list(week_map.values()))

            heat_mode = st.radio(
                "時段", ["全部", "白天 (06-18)", "夜間 (18-06)"], horizontal=True
            )

        # 依據使用者的 UI 選擇，層層過濾 DataFrame。
        df_filtered = df_raw.copy()
        if sel_year != "全部年份":
            df_filtered = df_filtered[df_filtered["Year"] == int(sel_year)]
        if sel_q != "全年":
            df_filtered = df_filtered[df_filtered["Quarter"] == int(sel_q.split()[1])]
        if sel_m != "全部":
            df_filtered = df_filtered[df_filtered["Month"] == int(sel_m.split()[0])]
        if sel_w != "全部":
            df_filtered = df_filtered[
                df_filtered["Weekday"] == {v: k for k, v in week_map.items()}[sel_w]
            ]
        if "白天" in heat_mode:
            df_filtered = df_filtered[
                (df_filtered["Hour"] >= 6) & (df_filtered["Hour"] < 18)
            ]
        elif "夜間" in heat_mode:
            df_filtered = df_filtered[
                (df_filtered["Hour"] >= 18) | (df_filtered["Hour"] < 6)
            ]

        df_trend_base = df_raw.copy()
        if sel_q != "全年":
            df_trend_base = df_trend_base[
                df_trend_base["Quarter"] == int(sel_q.split()[1])
            ]
        if sel_m != "全部":
            df_trend_base = df_trend_base[
                df_trend_base["Month"] == int(sel_m.split()[0])
            ]
        if sel_w != "全部":
            df_trend_base = df_trend_base[
                df_trend_base["Weekday"] == {v: k for k, v in week_map.items()}[sel_w]
            ]
        if "白天" in heat_mode:
            df_trend_base = df_trend_base[
                (df_trend_base["Hour"] >= 6) & (df_trend_base["Hour"] < 18)
            ]
        elif "夜間" in heat_mode:
            df_trend_base = df_trend_base[
                (df_trend_base["Hour"] >= 18) | (df_trend_base["Hour"] < 6)
            ]

    with col_right:
        if not df_filtered.empty:
            # 算平均值或直接計數
            city_current_totals = df_filtered.groupby("nightmarket_city")[
                metric_col
            ].agg(agg_func)

            # 利用 sort_values做降冪排列後，取 .iloc[0]，.iloc[-1] 代表第一名與最後一名
            sorted_totals = city_current_totals.sort_values(ascending=False)

            c_max = sorted_totals.iloc[0]  # 代表PDI分數最低或事件案件數最少，最佳！
            c_max_city = sorted_totals.index[0]

            c_min = sorted_totals.iloc[-1]  # 代表PDI分數最高、事件案件數最多，最差！
            c_min_city = sorted_totals.index[-1]

            # 計算全國平均
            national_avg = city_current_totals.mean()

            # 計算中位數，全台22個縣市，如果22個縣市都有參戰，擇取第十名；否則就抓"對半後取整數位"以後得到的那一名。
            mid_idx = 10 if len(sorted_totals) >= 11 else int(len(sorted_totals) / 2)
            mid_val = sorted_totals.iloc[mid_idx]
            mid_city = sorted_totals.index[mid_idx]

            # 不同指標的比較基準
            # 若為 PDI 指數，比較基準是「全國平均」；若為絕對數量(事故件數等)，比較基準改為「中位數縣市」，避免極端值(如雙北)拉高平均
            if is_pdi_mode:
                c_mid = national_avg  # 將平均值作為基準
                mid_city_display = "全國平均線"
                mid_tag_html = f"""<div style="font-size: 12px; color: #475569; background: #f1f5f9; padding: 2px 8px; border-radius: 4px; display: inline-block;">對標基準點：國均數 {national_avg:{fmt}} {unit}</div>"""
            else:
                c_mid = mid_val  # 將中位數作為基準
                mid_city_display = f"{mid_city}"  # 將中位數的對應縣市作為基準
                mid_diff = c_mid - national_avg  # 計算中位數與平均值差異
                mid_pct = (
                    (mid_diff / national_avg * 100) if national_avg > 0 else 0
                )  # 轉成差異百分比
                sign = "+" if mid_diff > 0 else ""
                mid_color = (
                    "#ef4444"
                    if mid_diff > 0
                    else ("#10b981" if mid_diff < 0 else "#475569")
                )
                mid_bg = (
                    "#fee2e2"
                    if mid_diff > 0
                    else ("#d1fae5" if mid_diff < 0 else "#f1f5f9")
                )
                mid_prefix = (
                    "高於國均"
                    if mid_diff > 0
                    else ("低於國均" if mid_diff < 0 else "持平國均")
                )
                mid_tag_html = f"""<div style="font-size: 12px; color: {mid_color}; background: {mid_bg}; padding: 2px 8px; border-radius: 4px; display: inline-block; font-weight: bold;">{mid_prefix} {sign}{mid_diff:{fmt}} ({mid_pct:+.1f}%)  |  國均數 {national_avg:{fmt}} {unit}</div>"""

            # 幫最好的一名計算，與平均值的差距
            max_diff = c_max - national_avg
            max_pct = (max_diff / national_avg * 100) if national_avg > 0 else 0
            max_sign = "+" if max_diff > 0 else ""
            max_prefix = (
                "高於國均"
                if max_diff > 0
                else ("低於國均" if max_diff < 0 else "持平國均")
            )

            # 幫最差的一名計算，與平均值的差距
            min_diff = c_min - national_avg
            min_pct = (min_diff / national_avg * 100) if national_avg > 0 else 0
            min_sign = "+" if min_diff > 0 else ""
            min_prefix = (
                "高於國均"
                if min_diff > 0
                else ("低於國均" if min_diff < 0 else "持平國均")
            )

            mode_title = "🚦 區域安全基準線 (以目前篩選條件下數據為準)"

            # UI 互動設計：HTML 客製化卡片
            # 建立三張並排的數據看板，利用 CSS Flexbox (display: flex; gap: 15px;) 達成自動排版與間距控制
            st.markdown(
                f"""
            <div style="margin-bottom: 25px;">
                <div style="font-size: 15px; color: #1e293b; font-weight: bold; margin-bottom: 12px;">{mode_title}</div>
                <div style="display: flex; gap: 15px;">
                    <div style="flex: 1; background-color: #ffffff; padding: 15px 20px; border-radius: 8px; border: 1px solid #e2e8f0; border-top: 4px solid #ef4444; box-shadow: 0 2px 4px rgba(0,0,0,0.02);">
                        <div style="font-size: 13px; font-weight: bold; color: #64748b; margin-bottom: 4px;">🚨 最高風險 (天花板)</div>
                        <div style="font-size: 18px; font-weight: 900; color: #1e293b; margin-bottom: 2px;">{c_max_city}</div>
                        <div style="font-size: 24px; font-weight: bold; color: #ef4444; margin-bottom: 6px;">{c_max:{fmt}} <span style="font-size:14px; font-weight:normal;">{unit}</span></div>
                        <div style="font-size: 12px; color: #ef4444; background: #fee2e2; padding: 2px 8px; border-radius: 4px; display: inline-block; font-weight: bold;">
                            {max_prefix} {max_sign}{max_diff:{fmt}} ({max_pct:+.1f}%)  |  國均數 {national_avg:{fmt}} {unit}
                        </div>
                    </div>
                    <div style="flex: 1; background-color: #ffffff; padding: 15px 20px; border-radius: 8px; border: 1px solid #e2e8f0; border-top: 4px solid #64748b; box-shadow: 0 2px 4px rgba(0,0,0,0.02);">
                        <div style="font-size: 13px; font-weight: bold; color: #64748b; margin-bottom: 4px;">🎯 {"中線基準 (Median)" if not is_pdi_mode else "全國平均線"}</div>
                        <div style="font-size: 18px; font-weight: 900; color: #1e293b; margin-bottom: 2px;">{mid_city_display}</div>
                        <div style="font-size: 24px; font-weight: bold; color: #334155; margin-bottom: 6px;">{c_mid:{fmt}} <span style="font-size:14px; font-weight:normal;">{unit}</span></div>
                        {mid_tag_html}
                    </div>
                    <div style="flex: 1; background-color: #ffffff; padding: 15px 20px; border-radius: 8px; border: 1px solid #e2e8f0; border-top: 4px solid #10b981; box-shadow: 0 2px 4px rgba(0,0,0,0.02);">
                        <div style="font-size: 13px; font-weight: bold; color: #64748b; margin-bottom: 4px;">🏆 最佳典範 (地板)</div>
                        <div style="font-size: 18px; font-weight: 900; color: #1e293b; margin-bottom: 2px;">{c_min_city}</div>
                        <div style="font-size: 24px; font-weight: bold; color: #10b981; margin-bottom: 6px;">{c_min:{fmt}} <span style="font-size:14px; font-weight:normal;">{unit}</span></div>
                        <div style="font-size: 12px; color: #10b981; background: #d1fae5; padding: 2px 8px; border-radius: 4px; display: inline-block; font-weight: bold;">
                            {min_prefix} {min_sign}{min_diff:{fmt}} ({min_pct:+.1f}%)  |  國均數 {national_avg:{fmt}} {unit}
                        </div>
                    </div>
                </div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        if not df_filtered.empty:
            st.markdown(
                '<div class="section-title">🔥 縣市排行 TOP 5</div>',
                unsafe_allow_html=True,
            )

            # 一次性計算出各縣市的事故總數、PDI平均、死傷總數，供後續卡片與表格使用
            rank_df = (
                df_filtered.groupby("nightmarket_city")
                .agg(
                    事故總數=("accident_id", "count"),
                    PDI平均=("pdi_score", "mean"),
                    死亡總數=("death_count", "sum"),
                    受傷總數=("injury_count", "sum"),
                )
                .reset_index()
                .rename(columns={"nightmarket_city": "城市"})
            )

            # 根據使用者選擇的分析指標連動，排名依據，且只回傳前五個最好的縣市。
            city_top5 = rank_df.sort_values(sort_col, ascending=False).head(5)

            rank_colors = [
                "linear-gradient(135deg, #1e3a8a, #1d4ed8)",
                "linear-gradient(135deg, #1e40af, #2563eb)",
                "linear-gradient(135deg, #1d4ed8, #3b82f6)",
                "linear-gradient(135deg, #2563eb, #60a5fa)",
                "linear-gradient(135deg, #3b82f6, #93c5fd)",
            ]

            cols_city = st.columns(5)

            # 計算全國平均
            national_avg_total = city_current_totals.mean()

            for idx, (_, row) in enumerate(city_top5.reset_index(drop=True).iterrows()):
                bg_color = rank_colors[idx]  # 為排名上色
                city_name = row["城市"]  # 佔據各名次的縣市名
                main_val = row[sort_col]  # 成績

                diff_val = main_val - national_avg_total
                diff_pct = (
                    (diff_val / national_avg_total * 100)
                    if national_avg_total > 0
                    else 0
                )

                if diff_val > 0:
                    nat_icon = "🔴"
                    nat_text = f"{nat_icon} 高於國均 {diff_val:{fmt}} {unit} <span style='font-size:10.5px; opacity:0.85;'>({diff_pct:+.1f}%)</span>"
                else:
                    nat_icon = "🟢"
                    nat_text = f"{nat_icon} 低於國均 {abs(diff_val):{fmt}} {unit} <span style='font-size:10.5px; opacity:0.85;'>({diff_pct:+.1f}%)</span>"

                with cols_city[idx]:
                    html_card = f"""<div class="pdi-card" style="background:{bg_color}; padding:15px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
<div style="font-size:12px; font-weight:bold; margin-bottom:5px; color:rgba(255,255,255,0.9);">🏆 第 {idx + 1} 名</div>
<div style="font-size:20px; font-weight:900; margin-bottom:5px; color:#ffffff;">{city_name}</div>
<div style="font-size:22px; font-weight:bold; margin-bottom:12px; color:#ffffff;">{main_val:{fmt}} <span style="font-size:13px;">{unit}</span></div>
<div style="font-size:12.5px; background:rgba(0,0,0,0.25); padding:6px 8px; border-radius:5px; color:#ffffff;">
{nat_text}
</div>
</div>"""
                    st.markdown(html_card, unsafe_allow_html=True)

            # st.markdown("<hr style='margin:15px 0;'>", unsafe_allow_html=True)

            # st.markdown(
            #     f"<div style='font-size:16px; font-weight:bold; color:#1f2937; margin-bottom:10px;'>📊 各縣市 {mode} 現況排名總表</div>",
            #     unsafe_allow_html=True,
            # )

            full_rank_df = rank_df.sort_values(sort_col, ascending=False).reset_index(
                drop=True
            )
            full_rank_df.insert(0, "排名", range(1, len(full_rank_df) + 1))

            full_rank_df["數值_str"] = full_rank_df[sort_col].apply(
                lambda x: f"{x:{fmt}}"
            )
            full_rank_df["排名_str"] = full_rank_df["排名"].astype(str)

            # 資料轉置：DataFrame Transpose (.T)
            # 將直式的資料表轉為橫式，展示多個縣市的單一指標比較
            df_transposed = (
                full_rank_df[["城市", "排名_str", "數值_str"]].set_index("城市").T
            )
            df_transposed.index = ["排名", f"{mode}"]

            with st.expander("點擊查看各縣市 綜合危險指數 (PDI) 今年度排名總表"):
                st.dataframe(df_transposed, width="content")

            st.markdown("<hr style='margin:15px 0;'>", unsafe_allow_html=True)

        st.markdown(
            '<div class="section-title" style="margin-bottom: 2px;">🗺️ 雙維度歷年矩陣對比 (嚴重度 vs 全台名次)</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            "<div style='font-size: 13px; font-weight: bold; color: #64748b; margin-bottom: 8px;'>🔍 快速定位區域</div>",
            unsafe_allow_html=True,
        )

        region_filter = st.radio(
            "定位區域",
            ["全台", "北部", "中部", "南部", "東部與東部離島", "其他離島"],
            horizontal=True,
            label_visibility="collapsed",
        )

        if not df_trend_base.empty:
            # 以縣市為 Y 軸，年份為 X 軸，數值為儲存格內容，建立熱力圖所需的矩陣資料(Pivot Table)
            heatmap_data = df_trend_base.pivot_table(
                index="nightmarket_city",
                columns="Year",
                values=metric_col,
                aggfunc=agg_func,
                fill_value=0,
            )

            if region_filter == "全台":
                target_regions = ["北部", "中部", "南部", "東部與東部離島", "其他離島"]
            else:
                target_regions = [region_filter]

            valid_cities = [
                city
                for region in target_regions
                for city in get_region_or_cities(region=region)
            ]
            available_cities = [c for c in valid_cities if c in heatmap_data.index]

            heatmap_data = heatmap_data.reindex(available_cities)

            # 動態計算熱力圖高度，避免縣市太多時文字擠在一起
            matrix_height = max(250, len(heatmap_data) * 35)

            st.markdown(
                f"<div style='text-align: center; font-size: 14px; font-weight: bold; color: #334155; margin-bottom: 5px;'>數值熱力圖 ({mode})</div>",
                unsafe_allow_html=True,
            )

            # Plotly 熱力圖
            # 利用顏色深淺呈現各縣市歷年的指標變化。colorscale='Blues' 數值越大顏色越深
            fig_heat_val = go.Figure(
                data=go.Heatmap(
                    z=heatmap_data.values,
                    x=heatmap_data.columns,
                    y=heatmap_data.index,
                    # 自訂 Blues：階與階之間的落差拉大，最深壓到 #2171b5 以提高層級鑑別度
                    colorscale=[
                        [0.00, "#f7fbff"],
                        [0.20, "#d6e6f5"],
                        [0.40, "#a9cce7"],
                        [0.60, "#74a9d8"],
                        [0.80, "#4585c4"],
                        [1.00, "#2171b5"],
                    ],
                    texttemplate="<b>%{z:,.2f}</b>"
                    if is_pdi_mode
                    else "<b>%{z:,.0f}</b>",
                    hovertemplate="年份: %{x}<br>縣市: %{y}<br>數值: %{z:,.2f}<extra></extra>"
                    if is_pdi_mode
                    else "年份: %{x}<br>縣市: %{y}<br>數值: %{z:,.0f}<extra></extra>",
                    # 中間調石板灰，深淺格子上都讀得到
                    textfont=dict(size=14, color="#475569"),
                )
            )

            fig_heat_val.update_layout(
                height=matrix_height,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis=dict(
                    side="top", tickmode="linear", dtick=1
                ),  # 把 X 軸(年份)移到上方
                yaxis=dict(categoryorder="array", categoryarray=available_cities[::-1]),
            )

            st.plotly_chart(fig_heat_val, width="stretch")


if __name__ == "__main__":
    main()
