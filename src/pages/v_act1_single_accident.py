"""單一夜市 AI 深度診斷頁：自訂半徑的周邊事故剖析與 Groq 生成報告。"""

import os
import re
import sys

import altair as alt
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from groq import Groq
from redis.exceptions import RedisError
from streamlit_folium import st_folium

import src.task.core.c_data_service as ds
import src.task.core.c_ui as ui
from src.task.core.c_db import get_night_markets_table
from src.util.logger_crtx import get_logger
from src.util.redis_utils import get_cache, set_cache

logger = get_logger(__name__)

load_dotenv()
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dotenv_path = os.path.join(current_dir, "..", ".env")
load_dotenv(dotenv_path=parent_dotenv_path, override=True)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

st.set_page_config(layout="wide", page_title="單一夜市事故AI分析", page_icon="📊")


# Redis 快取的欄位名沿用 analysis_pesdestrian_involving_accident，
# 與本頁沿用的舊表 tbl_accident_analysis_final 對不上，在此就地補齊：
#   accident_datetime ← accident_date + accident_time
#   Hour              ← accident_hourtime
#   primary_cause     ← cause_analysis_major_individual_grouped
def normalize_accident_columns(df):
    """把 Redis 快取的欄位名補成本頁使用的名稱。"""
    df = df.copy()
    if "accident_datetime" not in df.columns:
        df["accident_datetime"] = pd.to_datetime(
            df["accident_date"], errors="coerce"
        ) + pd.to_timedelta(df["accident_time"], errors="coerce")
    if "Hour" not in df.columns:
        df["Hour"] = pd.to_numeric(df["accident_hourtime"], errors="coerce")
    if "primary_cause" not in df.columns:
        # 取子類別（肇因研判子類別名稱-個別）。大類別 _grouped 那欄講的是用路人身份，不是肇因
        df["primary_cause"] = df["cause_analysis_minor_individual"]
    return df


# 從 Redis 提取大範圍包裹並執行局部裁切
# 從 Redis 撈出資訊，再利用 Haversine 距離公式動態過濾出使用者指定半徑內的事故


@st.cache_data(ttl=86400, show_spinner=False)
def get_single_market_redis(lat, lon, radius_km):
    """讀取該夜市 3 km 快取包裹，再裁切成指定半徑內的事故。

    快取「故障」與「未命中」語意分離（ADR-0003）：前者由 get_cache 拋
    RedisError 交呼叫端處理，後者才回傳空表。
    """
    cache_key = f"traffic:nearby_v12:{lat:.4f}_{lon:.4f}_3.0_all_sample"
    result = get_cache(cache_key)

    if isinstance(result, tuple) and len(result) >= 1:
        df = result[0]
    elif isinstance(result, pd.DataFrame):
        df = result
    else:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    distances = ds.haversine_distance(
        lat, lon, df["latitude"].values, df["longitude"].values
    )
    df_filtered = df[distances <= radius_km]
    return normalize_accident_columns(df_filtered)


def main():
    """繪製單一夜市 AI 深度診斷頁。"""
    st.markdown(
        """
    <style>
    .section-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; color: #333; margin-top: 15px; }
    div[data-testid="stVerticalBlock"] > div { padding-bottom: 0rem; }
    </style>
    """,
        unsafe_allow_html=True,
    )

    def get_all_nightmarkets():
        # 與 c_data_service.get_all_nightmarkets() 的 market:list_all_auto_v3 分開，
        cache_key = "market:list_all_page_v1"
        cached = get_cache(cache_key)
        if cached is not None:
            df_cached = pd.DataFrame(cached)
            if "AdminDistrict" in df_cached.columns and "Region" in df_cached.columns:
                return df_cached
        try:
            # 連線一律走 get_engine_to_mysql()（ADR-0004），連線資訊由 .env 提供
            df = get_night_markets_table()
            if df.empty:
                return df

            # 資料清洗：確保經緯度為數值型別，並補上四層級分類標籤供前端下拉選單使用
            df["lat"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["lon"] = pd.to_numeric(df["longitude"], errors="coerce")
            df["MarketName"] = df["nightmarket_name"]

            # 綁定四層級：
            df["Region"] = df["region"].replace(
                {"東部": "東部與離島", "離島": "東部與離島"}
            )
            df["City"] = df["city"]
            df["AdminDistrict"] = df["district"]

            # 處理附屬離島特例強制劃分
            df.loc[
                df["AdminDistrict"].str.contains("琉球|蘭嶼|綠島", na=False), "Region"
            ] = "東部與離島"
            df.loc[
                df["nightmarket_name"].str.contains("琉球|蘭嶼|綠島", na=False),
                "Region",
            ] = "東部與離島"

            # 向後相容舊程式碼
            df["District"] = df["Region"]
            df["District"] = df["Region"]

            result = df.dropna(subset=["lat", "lon"])  # 剔除經緯度遺漏的髒資料
            set_cache(cache_key, result.to_dict("records"), ttl=86400)
            return result
        except Exception:
            logger.error("夜市清單讀取失敗", exc_info=True)
            return pd.DataFrame()

    with ui.page_timer():
        df_market = get_all_nightmarkets()

    st.session_state["show_accidents"] = True
    _, _, layers = ui.render_sidebar(df_market)

    st.markdown(
        """
        <h2 style="margin-bottom: 5px;">🔍 單一夜市 AI 深度診斷</h2>
    """,
        unsafe_allow_html=True,
    )

    # 範圍定義與重疊差異的免責聲明
    st.info(
        "💡 **資料範圍說明**：\n本頁提供單一夜市的**深度與大範圍環境探索**。您可以透過左側滑桿自訂分析半徑（500m ~ 3km）。\n\n*註：為避免相鄰夜市的事故重複計算，其他頁面（全台總表、縣市對標）的總數與排名，皆採用嚴格的「500m 去重複核心區」計算。因此當您拉大本頁半徑，或加總多個夜市的數值時，將因包含重疊區域及外圍幹道，而大於排行榜之淨總數。*"
    )
    st.markdown(
        "<hr style='margin-top: 5px; margin-bottom: 20px;'>", unsafe_allow_html=True
    )

    col_left, col_right = st.columns([0.7, 2.3], gap="large")

    with col_left:
        st.markdown(
            '<div class="section-title" style="margin-top: 0;">📍 選擇分析目標</div>',
            unsafe_allow_html=True,
        )
        search_mode = st.radio(
            "尋找方式：",
            ["🔍 直接關鍵字搜尋", "🗺️ 區域篩選"],
            horizontal=True,
            label_visibility="collapsed",
        )

        def_market = "士林夜市"
        if search_mode == "🔍 直接關鍵字搜尋":
            all_markets = sorted(df_market["MarketName"].dropna().unique())
            sel_market = st.selectbox(
                "請輸入或選擇夜市名稱",
                all_markets,
                index=all_markets.index(def_market) if def_market in all_markets else 0,
            )
        else:
            with st.container(border=True):
                def_region, def_city, def_dist = "北部", "臺北市", "士林區"
                region_opts = sorted(df_market["Region"].dropna().unique())
                sel_region = st.selectbox(
                    "區域",
                    region_opts,
                    index=region_opts.index(def_region)
                    if def_region in region_opts
                    else 0,
                )

                city_opts = sorted(
                    df_market[df_market["Region"] == sel_region]["City"]
                    .dropna()
                    .unique()
                )
                sel_city = st.selectbox(
                    "縣市",
                    city_opts,
                    index=city_opts.index(def_city) if def_city in city_opts else 0,
                )

                dist_opts = sorted(
                    df_market[
                        (df_market["Region"] == sel_region)
                        & (df_market["City"] == sel_city)
                    ]["AdminDistrict"]
                    .dropna()
                    .unique()
                )
                sel_dist = st.selectbox(
                    "行政區",
                    dist_opts,
                    index=dist_opts.index(def_dist) if def_dist in dist_opts else 0,
                )

                m_opts = sorted(
                    df_market[
                        (df_market["Region"] == sel_region)
                        & (df_market["City"] == sel_city)
                        & (df_market["AdminDistrict"] == sel_dist)
                    ]["MarketName"]
                    .dropna()
                    .unique()
                )
                if not m_opts:
                    st.warning("此區域無夜市")
                    st.stop()
                sel_market = st.selectbox(
                    "夜市",
                    m_opts,
                    index=m_opts.index(def_market) if def_market in m_opts else 0,
                )

        target_market = df_market[df_market["MarketName"] == sel_market].iloc[0]

        # 清除跨夜市殘留的 AI 報告
        # 利用 Streamlit 的 session_state 紀錄上一次選擇的夜市。若發現夜市更換了，則清空舊的 AI 報告內容
        if "last_market" not in st.session_state:
            st.session_state.last_market = sel_market
        if st.session_state.last_market != sel_market:
            if "ai_report_text" in st.session_state:
                st.session_state.ai_report_text = ""
            st.session_state.last_market = sel_market

        st.markdown(
            '<div class="section-title">🎯 分析範圍與圖層</div>', unsafe_allow_html=True
        )
        with st.container(border=True):
            # 預設值保持 500m，呼應其他頁面的設定
            radius_m = st.slider(
                "選擇分析半徑 (公尺)",
                min_value=500,
                max_value=3000,
                step=500,
                value=500,
            )
            heat_mode = st.radio(
                "事故圖層時段",
                ["全部", "白天 (06-18)", "夜間 (18-06)"],
                horizontal=True,
            )

        radius_km = radius_m / 1000.0
        with st.spinner(f"正在載入 {sel_market} 周邊資料..."):
            try:
                df_raw = get_single_market_redis(
                    target_market["lat"], target_market["lon"], radius_km
                )
            except RedisError:
                # 快取「故障」與快取「未命中」自 ADR-0003 起語意分離：
                # 前者拋 RedisError，後者才會回傳空表。
                logger.error(
                    f"夜市周邊事故快取讀取失敗：{sel_market}",
                    exc_info=True,
                )
                st.error("⛔ 快取服務暫時無法使用，請稍後再試或聯繫維運人員。")
                st.stop()

        if df_raw.empty:
            st.error("⚠️ 該區域無事故資料，請嘗試擴大半徑。")
            st.stop()

        st.markdown(
            '<div class="section-title">📅 分析時間篩選</div>', unsafe_allow_html=True
        )
        with st.container(border=True):
            df_raw["accident_datetime"] = pd.to_datetime(df_raw["accident_datetime"])
            df_raw["Year"] = df_raw["accident_datetime"].dt.year
            df_raw["Quarter"] = df_raw["accident_datetime"].dt.quarter
            df_raw["Month"] = df_raw["accident_datetime"].dt.month
            df_raw["Weekday"] = df_raw["accident_datetime"].dt.weekday + 1

            yrs = sorted(df_raw["Year"].dropna().unique(), reverse=True)
            sel_y = st.selectbox("年份", ["全部年份"] + [str(int(y)) for y in yrs])
            sel_q = st.selectbox(
                "季度", ["全年", "第 1 季", "第 2 季", "第 3 季", "第 4 季"]
            )
            sel_m = st.selectbox("月份", ["全部"] + [f"{m} 月" for m in range(1, 13)])
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
            sel_w = st.selectbox("星期幾", list(week_map.values()))

    df_filtered = df_raw.copy()
    if sel_y != "全部年份":
        df_filtered = df_filtered[df_filtered["Year"] == int(sel_y)]
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

    if df_filtered.empty:
        with col_right:
            st.error("⚠️ 在您設定的篩選條件下，沒有發生任何事故記錄。")
        st.stop()

    # 此處為「拉桿範圍」的自訂數值
    total_count = len(df_filtered)
    dead_count = int(df_filtered["death_count"].sum())
    hurt_count = int(df_filtered["injury_count"].sum())

    if "pdi_score" in df_filtered.columns:
        local_pdi = df_filtered["pdi_score"].mean()
    else:
        df_filtered["weight"] = np.where(
            (df_filtered["Hour"] >= 17) | (df_filtered["Hour"] == 0), 1.5, 1.0
        )
        df_filtered["pdi_score"] = (
            df_filtered["death_count"] * 10 + df_filtered["injury_count"] * 2
        ) * df_filtered["weight"]
        local_pdi = df_filtered["pdi_score"].mean() if total_count > 0 else 0

    # 天氣與路面條件特徵擷取
    # 原始資料中的 weather_condition 是自由填寫的文字(如「雨」、「小雨」)。
    # 這裡利用 apply + lambda 搭配關鍵字比對 (in) 轉化為 0 或 1 的二元特徵 (is_rain, is_wet)，方便後續繪製雷達圖
    df_radar = df_filtered.copy()
    df_radar["weather_condition"] = df_radar["weather_condition"].fillna("")
    df_radar["light_condition"] = df_radar["light_condition"].fillna("")
    df_radar["road_surface_condition"] = df_radar["road_surface_condition"].fillna("")

    df_radar["is_rain"] = df_radar["weather_condition"].apply(
        lambda w: 1 if "雨" in w else 0
    )
    df_radar["is_dark"] = df_radar["light_condition"].apply(
        lambda x: 1 if any(k in x for k in ["暗", "夜", "未開啟", "無照明"]) else 0
    )
    df_radar["is_wet"] = df_radar["road_surface_condition"].apply(
        lambda r: 1 if any(k in r for k in ["濕", "積水"]) else 0
    )

    rain_ratio = df_radar["is_rain"].mean() * 100 if total_count > 0 else 0
    dark_ratio = df_radar["is_dark"].mean() * 100 if total_count > 0 else 0
    wet_ratio = df_radar["is_wet"].mean() * 100 if total_count > 0 else 0

    with col_right:
        # 事故總數不另立卡片：標題與件數各佔一欄，件數欄內文字靠右
        title_col, count_col = st.columns([2, 1], gap="small")
        with title_col:
            st.markdown(
                f"<h4 style='margin-top:0; margin-bottom:10px; color:#1e293b;'>🎯 {sel_market} 自訂周邊 {radius_m} 公尺安全體檢</h4>",
                unsafe_allow_html=True,
            )
        with count_col:
            st.markdown(
                f"<div style='text-align:right; font-size:15px; color:#64748b; padding-top:6px; white-space:nowrap;'>"
                f"📌 事故總數 <b style='color:#3b82f6; font-size:18px;'>{total_count:,}</b> 件</div>",
                unsafe_allow_html=True,
            )

        # 這裡下方開始的圖表，皆聯動左側拉桿的 custom 範圍
        chart_h = 240
        X_AXIS_H = (
            28  # Altair x 軸刻度文字加 tick 佔掉的高度，用來換算成與 plotly 相同的總高
        )
        c_radar, c_cause, c_hour = st.columns([1, 2, 1], gap=None)

        with c_radar:
            # Plotly 雷達圖 (Polar Line Chart)
            # 利用雷達圖呈現天氣 (雨天)、光線 (光線不佳)、路面 (濕滑) 三種環境變數的佔比情形，幫助直觀判斷環境劣勢
            st.markdown(
                "<div style='font-size:14px; font-weight:bold; color:#475569; text-align:center;'>🕸️ 環境風險雷達圖</div>",
                unsafe_allow_html=True,
            )
            risk_df = pd.DataFrame(
                {
                    "risk": ["雨天", "光線不佳", "濕滑路面"],
                    "value": [rain_ratio, dark_ratio, wet_ratio],
                }
            )
            fig = px.line_polar(
                risk_df,
                r="value",
                theta="risk",
                line_close=True,
                markers=True,
                range_r=[0, max(risk_df["value"].max() * 1.2, 10)],
            )
            fig.update_traces(
                fill="toself",
                marker=dict(color="#8E44AD", size=4),
                line=dict(color="#8E44AD"),
            )
            # 角度軸標籤（雨天／光線不佳／濕滑路面）畫在圓外側，margin 太小會被裁掉，
            # 故左右各留 35px；height 是總高，留邊只縮圓的大小，不影響與圓餅圖的下緣對齊
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(visible=False),
                    angularaxis=dict(tickfont=dict(size=10)),
                ),
                margin=dict(l=35, r=35, t=25, b=25),
                height=chart_h,
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)

        with c_cause:
            # Plotly 圓餅圖：Top 5 肇因，其餘併為「其他」，百分比才是佔全部事故的比例
            st.markdown(
                "<div style='font-size:14px; font-weight:bold; color:#475569; text-align:center;'>🔍 Top 5 肇因比例</div>",
                unsafe_allow_html=True,
            )
            if "primary_cause" in df_filtered.columns:
                cause_counts = df_filtered["primary_cause"].value_counts()
                df_cause = cause_counts.head(5).reset_index()
                df_cause.columns = ["肇因", "件數"]
                others = int(cause_counts.iloc[5:].sum())
                if others > 0:
                    df_cause.loc[len(df_cause)] = ["其他", others]

                # 肇因名稱最長逾 20 字，圖例放不下，截斷後以 hover 顯示全名
                df_cause["標籤"] = df_cause["肇因"].apply(
                    lambda c: c if len(c) <= 10 else c[:10] + "…"
                )
                fig_c = px.pie(
                    df_cause,
                    names="標籤",
                    values="件數",
                    custom_data=["肇因"],
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_c.update_traces(
                    textposition="inside",
                    textinfo="percent",
                    textfont_size=11,
                    hovertemplate="%{customdata[0]}<br>%{value} 件 (%{percent})<extra></extra>",
                )
                fig_c.update_layout(
                    height=chart_h,
                    margin=dict(l=0, r=0, t=0, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    legend=dict(
                        orientation="h",
                        font=dict(size=9),
                        yanchor="bottom",
                        y=1.02,
                        xanchor="center",
                        x=0.5,
                    ),
                )
                st.plotly_chart(fig_c, use_container_width=True)

        with c_hour:
            st.markdown(
                "<div style='font-size:14px; font-weight:bold; color:#475569; text-align:center;'>🌙 24H 事故熱力</div>",
                unsafe_allow_html=True,
            )
            df_h = df_filtered.groupby("Hour").size().reset_index(name="n")
            h_chart = (
                alt.Chart(df_h)
                .mark_area(color="lightblue", line={"color": "#2563eb"}, opacity=0.6)
                .encode(
                    x=alt.X(
                        "Hour:O",
                        title=None,
                        axis=alt.Axis(
                            labelAngle=0, labelFontSize=10, values=[0, 6, 12, 18, 23]
                        ),
                    ),
                    y=alt.Y("n:Q", title=None, axis=alt.Axis(labels=False)),
                )
            )
            # Altair 的 height 只算繪圖區，x 軸標籤與 padding 會另外往下長；
            # plotly 的 height 則是含全部的總高。扣掉 X_AXIS_H 才能與圓餅圖下緣切齊
            st.altair_chart(
                h_chart.properties(height=chart_h - X_AXIS_H, padding=0).configure_view(
                    strokeWidth=0
                ),
                use_container_width=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # 效能保護降級機制
        # 若點位過多 (超過 1000 個點)，Folium 地圖會讓瀏覽器卡死
        # 因此這裡優先保留所有死亡事故點位，一般事故則隨機抽樣 1500 點進行渲染地圖，平衡視覺豐富度與網頁效能
        df_for_map = df_filtered.copy()
        if len(df_for_map) > 1000:
            df_death_map = df_for_map[df_for_map["death_count"] > 0]
            df_other_map = df_for_map[df_for_map["death_count"] == 0]
            if len(df_other_map) > 1500:
                df_other_map = df_other_map.sample(n=1500, random_state=42)
            df_for_map = pd.concat([df_death_map, df_other_map])

        d_zoom = (
            16
            if radius_m <= 500
            else 15
            if radius_m <= 1000
            else 14
            if radius_m <= 2000
            else 13
        )
        m = ui.build_map(
            False,
            target_market,
            layers,
            d_zoom,
            radius_m,
            None,
            df_for_map,
            df_market,
            custom_tiles="OpenStreetMap",
        )
        st_folium(m, height=400, use_container_width=True, returned_objects=[])

        # AI 應答區塊置於地圖下方，與地圖同寬
        st.markdown(
            """
            <div style="background-color: #f8fafc; padding: 15px; border-radius: 12px; border: 1px solid #cbd5e1; margin-top: 15px;">
                <div style="font-size:16px; font-weight:bold; color:#1f2937; margin-bottom:8px;">🤖 專屬 AI 深度分析</div>
                <div style="font-size:13px; color:#475569; margin-bottom:15px; line-height:1.5;">綜合地圖熱點、時段與環境變數，產生防護建議。</div>
        """,
            unsafe_allow_html=True,
        )

        top_cause_str = (
            df_filtered["primary_cause"].value_counts().idxmax()
            if not df_filtered.empty
            else "未知"
        )
        peak_hour_str = (
            df_filtered.groupby("Hour").size().idxmax()
            if not df_filtered.empty
            else "未知"
        )

        df_death = df_filtered[df_filtered["death_count"] > 0]
        if not df_death.empty:
            r_lat, r_lon = df_death.groupby(["latitude", "longitude"]).size().idxmax()
            risky_loc = f"https://www.google.com/maps/search/?api=1&query={r_lat},{r_lon} (曾發生死亡事故)"
        elif not df_filtered.empty:
            r_lat, r_lon = (
                df_filtered.groupby(["latitude", "longitude"]).size().idxmax()
            )
            risky_loc = f"https://www.google.com/maps/search/?api=1&query={r_lat},{r_lon} (高頻事故熱點)"
        else:
            risky_loc = "無明顯熱點"

        if st.button("✨ 立即生成分析報告", type="primary", use_container_width=True):
            with st.spinner("AI 正在解讀地圖與數據..."):
                try:
                    st.session_state.ai_report_text = get_ai_analysis(
                        target_market["MarketName"],
                        total_count,
                        local_pdi,
                        dead_count,
                        hurt_count,
                        top_cause_str,
                        peak_hour_str,
                        rain_ratio,
                        dark_ratio,
                        wet_ratio,
                        risky_loc,
                    )
                except Exception:
                    # 前端是例外停止傳播之處，須完整記錄（ADR-0003）
                    logger.error(
                        f"AI 分析報告生成失敗：{target_market['MarketName']}",
                        exc_info=True,
                    )
                    st.session_state.ai_report_text = ""
                    st.error("⛔ AI 服務暫時無法使用，請稍後再試或聯繫維運人員。")

        if st.session_state.get("ai_report_text"):
            # AI 回傳的是不可信內容，一律以 Markdown 渲染，不得走 unsafe_allow_html
            st.markdown(st.session_state.ai_report_text)

        st.markdown("</div>", unsafe_allow_html=True)


# GROQ AI 生成單一夜市分析內容
# 串接 Groq API，將複雜的數據(死傷數、天氣比例、尖峰時刻、主要肇因等)包裝進 Prompt


@st.cache_data(ttl=3600, show_spinner=False)
def get_ai_analysis(
    market_name,
    total,
    pdi,
    dead,
    hurt,
    top_cause,
    peak_hour,
    rain,
    dark,
    wet,
    risky_loc,
):
    """把該夜市的統計數據包成 prompt，交給 Groq 生成防護建議。

    例外一律往上拋，由呼叫端（前端邊界）決定如何降級（ADR-0003）。
    """
    api_key = os.getenv("GROQ_API_KEY")
    # 必填設定在真正要用的那一刻驗證，避免缺設定被下游的認證錯誤掩蓋（ADR-0008）
    if not api_key:
        raise ValueError("未設定 GROQ_API_KEY，請檢查環境變數設置")

    client = Groq(api_key=api_key)
    prompt = f"""
        你是一個交通專家。請分析「{market_name}」數據（總字數限制150字內）。
        數據：總事故{total}件、PDI指數{pdi:.2f}、死亡{dead}、受傷{hurt}。榜首肇因：{top_cause}。尖峰：{peak_hour}點。
        環境：雨天{rain:.1f}%、昏暗{dark:.1f}%、路濕{wet:.1f}%。
        最危險熱點：{risky_loc}

        請回答五大重點，且每個重點之間必須使用 Markdown 的雙換行 (`\n\n`) 來隔開：
        1. 肇因與預防。
        2. 綜合環境風險。
        3. 路段特徵推測。
        4. 熱點改善對策（附帶 Google Maps 網址連結）。
        5. 安全總結標語。
        格式：請用 Markdown 條列式，語氣專業，每次生成的內容，請固定都用 - 並且字體大小要相同。
        """
    res = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="llama-3.3-70b-versatile",
    )
    content = res.choices[0].message.content

    # 將連續單一換行取代為雙換行，確保 Streamlit 渲染出正確段落
    return re.sub(r"(?<!\n)\n(?!\n)", "\n\n", content)


if __name__ == "__main__":
    main()
