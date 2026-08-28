"""共用 UI 元件：側邊欄、效能計時、Folium 地圖、翻譯掛件與共用 CSS。

各分頁重複用得到的 Streamlit、Folium 與 CSS 片段都集中在這裡：

1. `render_sidebar()` —— 所有分頁共用的左側導航欄
2. `page_timer()` —— 量測一段程式碼的執行時間
3. `build_map()` —— 依參數畫出全臺總覽熱力圖或單一夜市細節圖
4. `render_google_translator()` —— 嵌入 Google 翻譯掛件
5. `load_custom_css()` —— 載入各頁共用的樣式

一個分頁的典型流程是：載入資料 → 呼叫 `render_sidebar()` 與
`load_custom_css()` → 準備好篩選後的資料 → 呼叫 `build_map()` →
以 `st_folium()` 顯示回傳的地圖物件。
"""

import time
import uuid  # to create a unique HTML id for the Google Translate container
from contextlib import contextmanager  # to create a custom with....: block

import folium
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components  # to embed raw HTML/JS into streamlit
from folium.plugins import (  # two plugins used for heatmaps and clustered markers.
    HeatMap,
    MarkerCluster,
)


# 1. 側邊欄 (Sidebar)
def render_sidebar() -> dict:
    """畫出所有分頁共用的左側導航欄，回傳預設的地圖圖層開關。

    Returns:
        dict: 圖層開關，例如：

            {
                "traffic_heat": True,
                "night_market": True,
                "weather": False,
                "accidents": True,
            }
    """
    # 呼叫語言切換選單
    st.sidebar.markdown("### 🌐 語言切換 / Language")
    render_google_translator()

    st.sidebar.page_link("app.py", label="首頁", icon="🏠")
    # 側邊欄結構
    st.sidebar.markdown("#### 車禍與夜市關聯性分析")
    st.sidebar.page_link(
        "pages/v_act1_all_accident.py", label="全台夜市事故總體檢", icon="🗺️"
    )
    st.sidebar.page_link(
        "pages/v_act1_city_accident.py", label="縣市安全對標與趨勢", icon="🏙️"
    )
    st.sidebar.page_link(
        "pages/v_act1_single_accident.py", label="單一夜市 AI 深度診斷", icon="🔍"
    )
    st.sidebar.markdown("#### 無標的夜市下之車禍分析")
    st.sidebar.page_link(
        "pages/v_act2_tableau.py", label="全國區域 - 修法前後分析研究", icon="📈"
    )

    # 預設地圖圖層的開關狀態
    layers = {
        "traffic_heat": True,
        "night_market": True,
        "weather": False,
        "accidents": True,
    }
    return layers


# 效能計時函式搭配上下文管理器decorator
# 效果是，可使用with 效能計時函式(): 下面包住一段程式碼，便可計算該區塊的執行時間，進而用於評估效能調校前後差。
@contextmanager
def page_timer():
    """量測 `with` 區塊內程式碼的執行時間的上下文管理器。

    用來評估效能調校前後的差異，目前只計算不顯示，需要看數字時在此加上輸出。

    Yields:
        None: 進入區塊時開始計時，離開時結束計時。

    Examples:
        >>> with page_timer():
        ...     df = load_data()
    """
    start_time = time.time()  # Records the current time before the wrapped code runs
    yield  # Pauses here and lets the code inside with ui.page_timer(): execute.
    end_time = time.time()  # Runs after the wrapped block finishes
    _ = (
        end_time - start_time
    )  # Calculates elapsed time, but not intend to use this value.


# 地圖
def build_map(
    is_overview,
    target_market: dict,
    layers: dict,
    dynamic_zoom,
    radius_m: int,
    traffic_global: pd.DataFrame,
    df_local: pd.DataFrame,
    df_market: pd.DataFrame,
    custom_tiles="CartoDB positron",
) -> folium.Map:
    """畫出全臺總覽熱力圖或單一夜市細節圖，回傳 Folium 地圖物件。

    視角依情境決定：總覽模式定在臺灣中部、縮放 8；指定夜市時綁定該夜市座標；
    兩者皆無則落在臺北、縮放 12。地圖最多疊三組圖層，各自受 `layers` 開關控制：

    - 全臺事故熱力圖。
    - 夜市點位：指定夜市時畫星標與分析範圍圓圈，否則畫出所有夜市的小圓點。
    - 在地事故點位（僅非總覽模式）：拆成一般事故與死亡事故兩群，死亡事故以
      紅色光暈標記強制疊在最上層。一般事故超過 800 筆時自動改用熱力圖呈現，
      以免瀏覽器卡死，未超過則以叢集點位顯示。

    回傳的物件交由呼叫端以 `st_folium()` 顯示。

    Args:
        is_overview (bool): 是否為全臺總覽模式。
        target_market (dict): 指定的夜市，須含 `lat`、`lon` 與 `MarketName`；
            總覽模式可傳 `None`。
        layers (dict): 圖層開關，即 `render_sidebar()` 的回傳值。
        dynamic_zoom (int | None): 單一夜市模式的縮放層級，`None` 時為 16。
        radius_m (int): 分析範圍圓圈的半徑，單位公尺。
        traffic_global (pandas.DataFrame): 全臺熱力圖資料，空值則不畫該圖層。
        df_local (pandas.DataFrame): 在地事故資料，至少須含 `latitude`、
            `longitude` 與 `death_count`。
        df_market (pandas.DataFrame): 夜市資料，至少須含 `lat`、`lon` 與
            `MarketName`，通常來自 `get_nightmarkets_for_page_selector()`。
        custom_tiles (str): 地圖底圖樣式，預設 `"CartoDB positron"`。

    Returns:
        folium.Map: 已疊好圖層的地圖物件。

    Raises:
        KeyError: `target_market` 或 `df_market` 缺少座標與名稱欄位。
    """
    # 依據傳入的參數決定要畫「全台總覽熱力圖」還是「單一夜市細節圖」
    # 視角初始化

    if is_overview:
        loc, zoom = [23.7, 120.95], 8  # 如果是全台總覽，中心定在台灣中部
    elif target_market is not None:
        loc = [
            target_market["lat"],
            target_market["lon"],
        ]  # ；如果是看單一夜市，則將地圖中心綁定到該夜市經緯度
        # 接收 v_act1_single_accident 傳來的動態縮放值，若無則預設 16
        zoom = dynamic_zoom if dynamic_zoom is not None else 16
    else:
        loc, zoom = [25.03, 121.56], 12  # 臺北

    # 地圖初始化。
    m = folium.Map(
        location=loc, zoom_start=zoom, tiles=custom_tiles, prefer_canvas=True
    )

    # [圖層 1] 全台交通熱力圖
    if (
        layers.get("traffic_heat") and traffic_global
    ):  # 若traffic_heat圖層enabled且traffic_global有數據
        HeatMap(traffic_global, radius=15, blur=12, min_opacity=0.3).add_to(m)

    # [圖層 2] 夜市點位標示，設計為三個圖層組成的圖層群組。
    if layers.get("night_market"):  # and ....?
        # 建立群組
        fg_m = folium.FeatureGroup(name="夜市周圍邊界")

        if target_market is not None:  # 針對單一夜市畫
            # 群組圖層1，星星標記點。
            folium.Marker(
                [target_market["lat"], target_market["lon"]],
                icon=folium.Icon(
                    color="purple", icon="star", prefix="fa"
                ),  # 紫色星星圖標
                tooltip=target_market["MarketName"],
            ).add_to(fg_m)  # 滑鼠移過去會顯示的文字

            # 群組圖層2，橘色分析範圍圓圈
            folium.Circle(
                [target_market["lat"], target_market["lon"]],
                radius=radius_m,
                color="orange",
                fill=True,
                fill_opacity=0.1,
            ).add_to(fg_m)
        else:
            # 群組圖層3，畫出全台所有夜市的紫小圓點，點數不多、每個點如果獨立顯示並不會顯得很亂的話，可以不需事先加上MarkerCluster()
            for _, r in df_market.iterrows():
                folium.CircleMarker(
                    [r["lat"], r["lon"]],
                    radius=3,
                    color="purple",
                    tooltip=r["MarketName"],
                ).add_to(fg_m)
        # 將群組加入地圖物件
        fg_m.add_to(m)

    # [圖層 3] 在地事故點位
    if (
        (not is_overview)
        and (layers.get("accidents"))
        and (df_local is not None)
        and (not df_local.empty)
    ):
        # 拆分為致死事故(mortal)、一般事故(not_mortal)
        df_mortal = df_local[df_local["death_count"] > 0]
        df_not_mortal = df_local[df_local["death_count"] == 0]

        # 建立一般事故的圖層群組，由兩個圖層組成。
        fg_not_mortal = folium.FeatureGroup(name="一般受傷事故")

        # [效能防護機制] 如果單一區域事故 > 800 筆，為避免瀏覽器卡死，自動降級為熱力圖呈現；否則使用叢集點位
        if len(df_not_mortal) > 800:
            # [圖層1] HeatMap
            heat_data = [[r.latitude, r.longitude] for r in df_not_mortal.itertuples()]
            HeatMap(heat_data, radius=12, blur=15, min_opacity=0.3).add_to(
                fg_not_mortal
            )
        else:
            # [圖層2] CircleMarker
            # 製作容器
            cluster_other = MarkerCluster(
                maxClusterRadius=30, disableClusteringAtZoom=16
            ).add_to(fg_not_mortal)

            # 繪製點並放入容器
            for r in df_not_mortal.itertuples():
                i_count = getattr(r, "injury_count", 0)
                color = "blue" if i_count > 0 else "black"  # 有受傷標藍色，僅財損標黑色
                cause = getattr(r, "cause_analysis_major_individual_grouped", "未知")

                dt_date = getattr(r, "accident_date", None)
                dt_time = getattr(r, "accident_time", None)
                dt_date_str = (
                    dt_date.strftime("%Y-%m-%d") if pd.notnull(dt_date) else "未知日期"
                )
                # accident_time 在 MySQL 是 TIME、讀進來是 Timedelta（沒有 strftime），
                # 用 str() 取尾段可同時吃 Timedelta、Timestamp 與 datetime.time
                dt_time_str = (
                    str(dt_time).split()[-1] if pd.notnull(dt_time) else "未知時間"
                )
                dt_str = dt_date_str + " " + dt_time_str

                popup_text = f"一般事故<br>{dt_str}<br>{cause}<br>傷:{i_count}"
                folium.CircleMarker(
                    [r.latitude, r.longitude],
                    radius=5,
                    color=color,
                    fill=True,
                    fill_opacity=0.7,
                    popup=folium.Popup(popup_text, max_width=200),
                ).add_to(cluster_other)
        fg_not_mortal.add_to(m)

        # 建立死亡事故的圖層群組，由1個圖層組成，未來有想法再擴充。
        if not df_mortal.empty:
            fg_death = folium.FeatureGroup(name="死亡事故")
            # [圖層1] Marker
            for r in df_mortal.itertuples():
                d_count = getattr(r, "death_count", 0)
                i_count = getattr(r, "injury_count", 0)
                cause = getattr(r, "cause_analysis_major_individual_grouped", "未知")

                dt_date = getattr(r, "accident_date", None)
                dt_time = getattr(r, "accident_time", None)
                dt_date_str = (
                    dt_date.strftime("%Y-%m-%d") if pd.notnull(dt_date) else "未知日期"
                )
                # accident_time 在 MySQL 是 TIME、讀進來是 Timedelta（沒有 strftime），
                # 用 str() 取尾段可同時吃 Timedelta、Timestamp 與 datetime.time
                dt_time_str = (
                    str(dt_time).split()[-1] if pd.notnull(dt_time) else "未知時間"
                )
                dt_str = dt_date_str + " " + dt_time_str

                popup_text = (
                    f"🚨 死亡事故<br>{dt_str}<br>{cause}<br>死:{d_count} 傷:{i_count}"
                )

                # CSS：使用客製化 HTML DivIcon 創造類似警示燈的紅色圓點
                # box-shadow 製造光暈效果；z_index_offset=1000 強制讓死亡事故疊加在所有一般事故之上，突出其嚴重性
                icon_html = '<div style="background-color: #ff0000; width: 16px; height: 16px; border-radius: 50%; border: 2px solid white; box-shadow: 0 0 6px rgba(0,0,0,0.8);"></div>'

                folium.Marker(
                    [r.latitude, r.longitude],
                    icon=folium.DivIcon(html=icon_html, icon_anchor=(8, 8)),
                    popup=folium.Popup(popup_text, max_width=200),
                    z_index_offset=1000,  # 強制永遠顯示在其他點位之上
                ).add_to(fg_death)

            fg_death.add_to(m)

        # 最後加入圖層控制面板 (地圖右上角)
        folium.LayerControl(collapsed=False).add_to(m)

    return m  # 將畫好的地圖交還給主程式


# 透過插入Google Translate的 JS 腳本，達成多國語言翻譯
def render_google_translator():
    """在側邊欄插入 Google 翻譯的語言切換掛件。

    掛件實際上要注入到最頂層視窗才有作用，因此以 JavaScript 跨過 Streamlit 的
    iframe 限制建立元件，再定期檢查 DOM 把它搬回側邊欄中的指定位置（最多嘗試
    50 次）。提供繁體中文、英文、日文與韓文四種語言。
    """
    container_id = f"google_translate_{uuid.uuid4().hex}"
    st.sidebar.markdown(f'<div id="{container_id}"></div>', unsafe_allow_html=True)
    st.sidebar.markdown("---")

    components.html(
        f"""
        <script>
        // 利用 window.parent 跨越 Streamlit iframe 的限制，將翻譯工具注入到最頂層視窗
        const parentWindow = window.parent;
        const parentDoc = parentWindow.document;

        if (!parentWindow.persistent_google_translate) {{
            parentWindow.persistent_google_translate = parentDoc.createElement('div');
            parentWindow.persistent_google_translate.id = 'persistent_google_translate';

            parentWindow.googleTranslateElementInit = function() {{
                new parentWindow.google.translate.TranslateElement({{
                    pageLanguage: 'zh-TW',
                    includedLanguages: 'zh-TW,en,ja,ko',
                    layout: parentWindow.google.translate.TranslateElement.InlineLayout.SIMPLE
                }}, 'persistent_google_translate');
            }};

            const script = parentDoc.createElement('script');
            script.id = 'google-translate-script';
            script.src = 'https://translate.google.com/translate_a/element.js?cb=googleTranslateElementInit';
            parentDoc.body.appendChild(script);
        }}

        // 定期檢查 DOM，把翻譯元件搬回 Streamlit Sidebar 中的指定位置
        let attempts = 0;
        const timer = setInterval(() => {{
            const newContainer = parentDoc.getElementById('{container_id}');
            if (newContainer && parentWindow.persistent_google_translate) {{
                newContainer.appendChild(parentWindow.persistent_google_translate);
                clearInterval(timer);
            }}
            attempts++;
            if (attempts > 50) clearInterval(timer);
        }}, 100);
        </script>
        """,
        height=0,
        width=0,
    )


# CSS：統整所有頁面的卡片、標題、KPI 樣式
def load_custom_css():
    """載入各頁面共用的卡片、標題與 KPI 樣式。

    以 `st.markdown()` 注入一段 CSS，內容涵蓋 PDI 危險指數卡片（含滑鼠停留時的
    懸浮效果）、標題強調色，以及縣市比較頁面用的 KPI 數據方塊。數據方塊的漲跌
    以綠升紅降表示。
    """
    st.markdown(
        """
    <style>
        /* 共用：PDI 危險指數卡片 */
        /* hover 設定創造懸浮感 (transform: translateY)，提升質感與可點擊提示 */
        .pdi-card { padding: 18px; border-radius: 12px; box-shadow: 0 4px 10px rgba(0,0,0,0.15); transition: 0.2s; height: 100%; color: white; margin-bottom: 10px;}
        .pdi-card:hover { transform: translateY(-3px); box-shadow: 0 6px 15px rgba(0,0,0,0.25); }

        /* 共用：標題與區塊排版 */
        /* 使用主色調 #e11d48 (搶眼的玫瑰紅) 強調重點標題 */
        .title-highlight { color: #e11d48; font-weight: bold; }
        .section-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; color: #333; }
        div[data-testid="stVerticalBlock"] > div { padding-bottom: 0rem; }

        /* 共用：KPI 數據方塊 (用於各縣市比較頁面) */
        /* 以淺灰底與圓角建構類似儀表板 (Dashboard) 的數據塊 */
        .kpi-box { background-color: #f8f9fa; padding: 10px; border-radius: 8px; text-align: center; border: 1px solid #e5e7eb; }
        .kpi-title { font-size: 13px; color: #6b7280; margin-bottom: 2px; }
        .kpi-value { font-size: 22px; font-weight: bold; color: #111827; }
        .kpi-delta { font-size: 12px; font-weight: bold; }
        /* 綠升紅降，符合投資/數據看板的直覺認知 */
        .delta-good { color: #10b981; }
        .delta-bad { color: #ef4444; }
    </style>
    """,
        unsafe_allow_html=True,
    )
