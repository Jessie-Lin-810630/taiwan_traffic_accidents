"""Streamlit 首頁：全臺夜市交通安全總覽。

整個前端的進入點，以 `streamlit run src/app.py` 啟動。頁面本身只做總覽與導覽：
顯示全臺事故熱力圖與關鍵數字，並提供前往各分析分頁的入口。實際的分頁在
`src/pages/`，由 Streamlit 依檔名自動掛上。
"""

import time

import streamlit as st

import src.task.core.c_data_service as ds
import src.task.core.c_ui as ui
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

# # 如果streamlit run時找不到src.，可嘗試強制校準，將專案根目錄插在sys.path最前面，讓streamlit可以從這層往下找得到src
# import sys
# import os
# from pathlib import Path
# root = Path(__file__).resolve().parent.parent
# if str(root) not in sys.path:
#     sys.path.insert(0, str(root))

# 然後才import模組、套件


# 1. 頁面設定，set_page_config()通常是第一手要寫的
st.set_page_config(
    layout="wide",  # 將網頁元素撐滿瀏覽器視窗
    page_title="臺灣夜市風險地圖",  # 網頁標籤頁要顯示的名稱
    page_icon="🚦",  # 網頁標籤頁icon
    initial_sidebar_state="expanded",
)  # 側邊欄的收合方式
#   # initial_sidebar_state側邊欄的收合方式，預設 "auto"，代表要根據瀏覽器頁面寬度自動決定是否顯示:
#   # 在大畫面螢幕時顯示側邊欄，在小畫面螢幕(例如手機)會將側邊欄隱藏為漢堡選單)。
#   # 也可以設為 "expanded"，代表完全不收合。


def main():
    """組出首頁版面，載入全臺事故與夜市資料。

    流程是：注入首頁樣式 → 顯示 2.5 秒的載入動畫 → 呼叫資料服務層預熱 Redis
    快取並取得夜市主檔 → 畫出側邊欄 → 依序排出主視覺、痛點引言、受眾價值卡片
    與各分頁的入口按鈕。

    資料服務層取數失敗時，前端是例外停止傳播之處，因此完整記錄後顯示錯誤訊息
    並中止本次渲染，不讓頁面帶著半套資料繼續畫。

    Notes:
        前端負責決定如何降級，參考 ADR-0003。
    """
    # 2. 樣式設定，影響61行以後的排版
    # markdown()比st.write()、st.title()、st.header()、st.subheader()有較高客製化的能力，但也比較不易讀。
    # markdown()可以讓streamlit使用原生HTML/CSS標記式語言來渲染網頁。
    st.markdown(
        """
                <style>
                    .hero-metric-box {
                        text-align: center;
                        padding: 20px;
                        background: #ffffff;
                        border-radius: 12px;
                        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                        border: 1px solid #e2e8f0;
                    }
                    .action-title { font-size: 1.1rem; color: #334155; font-weight: bold; margin-bottom: 8px; }
                    .action-desc { color: #64748b; font-size: 0.95rem; margin-bottom: 15px; line-height: 1.5; }
                </style>
                """,
        unsafe_allow_html=True,
    )

    # 3. 載入動畫
    splash = st.empty()  # 告訴python，要在網頁切出一個空的容器，稍後會有進一步指令，例如，往容器內填寫、取代、清掉...
    with splash.container():  # 指定splash 容器內可以放入很多元素，如果直接splash.markdown()，會有後面覆蓋前面的問題。
        st.markdown(
            """
                        <style>
                            .splash-container { display: flex; flex-direction: column; justify-content: center; align-items: center; height: 70vh; background-color: #ffffff; animation: fadeIn 1s; }
                            .loading-text { color: #444; font-family: "Microsoft JhengHei", sans-serif; margin-top: 20px; }
                            .sub-text { color: #888; font-size: 0.9em; }
                        </style>
                        <div class="splash-container">
                            <img src="https://media.giphy.com/media/l0HlOaQcLJ2hHpYcw/giphy.gif" width="150" style="border-radius: 10px;">
                            <h2 class="loading-text">🚦 正在分析全台路況數據...</h2>
                            <p class="sub-text">整合 <b>150 萬筆</b> 交通事故 x <b>300+個</b> 夜市位置 x 天氣資料</p>
                            <div style="width: 300px; height: 4px; background: #eee; margin-top: 15px; border-radius: 2px;">
                                <div style="width: 100%; height: 100%; background: #ef4444; animation: loading 2s infinite;"></div>
                            </div>
                            <style>@keyframes loading { 0% { width: 0%; } 50% { width: 70%; } 100% { width: 100%; } }</style>
                        </div>
                    """,
            unsafe_allow_html=True,
        )
        time.sleep(
            2.5
        )  # 暫停2.5秒，讓splash內的元素(動畫)被清掉(下一行的關係)之前可以留在畫面上。
    splash.empty()  # 清掉splash容器。

    # 4. 取得全台資料，跟前端網頁渲染無關
    # 資料服務層自 ADR-0003 起一律拋出例外，由前端決定如何降級。
    try:
        ds.get_accident_heatmap_data()  # 僅為預熱 Redis 快取，回傳值本頁未使用
        df_market = ds.get_all_nightmarkets()
    except Exception:
        # 前端是例外停止傳播之處，須完整記錄（ADR-0003）
        logger.error("首頁資料載入失敗", exc_info=True)
        st.error("⛔ 資料服務暫時無法使用，請稍後再試或聯繫維運人員。")
        st.stop()

    # 5. 呼叫側邊欄
    is_overview, target_market, layers = ui.render_sidebar(df_market)

    # 6. 主頁排版：縮排集中視覺 (控制整體最大寬度)

    # 將網頁切成由左而右三容器，0.5、9、0.5代表三個容器的寬度比例為5 : 90 : 5
    spacer_left, col_main, spacer_right = st.columns([0.5, 9, 0.5])

    # 排中間容器，以下分成上1、上2、中1、中2、中3、下層區塊
    with col_main:  # "with 容器變數" 用以往內塞入元素
        # --- 上1區塊：Hero 視覺與大標題 ---
        st.markdown(
            """
                        <div style="text-align: center;">
                            <h1 style='color: #0f172a; font-size: 3.2rem; font-weight: 900; margin-bottom: 0px;'>
                                你的生活離<span style='color: #ef4444;'>行人地獄</span>有多遠❗❓<br>
                                <p>
                                數據揭露的真相
                                </p>
                            </h1>
                        </div>
                    """,
            unsafe_allow_html=True,
        )

        # 緊接著再由左而右切出三個儀表板
        m1, m2, m3 = st.columns(
            3, gap="medium"
        )  # 3代表切出三個等寬的容器，gap描述容器之間間距medium
        with m1:
            st.markdown(
                "<div class='hero-metric-box'><div style='color: #64748b; font-size: 16px; font-weight:bold;'>涵蓋全台夜市</div><div style='color: #0f172a; font-size: 38px; font-weight: 900;'>300<span style='color:#3b82f6;'>+</span> 處</div></div>",
                unsafe_allow_html=True,
            )
        with m2:
            st.markdown(
                "<div class='hero-metric-box'><div style='color: #64748b; font-size: 16px; font-weight:bold;'>分析交通事故</div><div style='color: #0f172a; font-size: 38px; font-weight: 900;'>150<span style='color:#3b82f6;'> 萬+</span> 件</div></div>",
                unsafe_allow_html=True,
            )
        with m3:
            st.markdown(
                "<div class='hero-metric-box'><div style='color: #64748b; font-size: 16px; font-weight:bold;'>追蹤分析區間</div><div style='color: #0f172a; font-size: 38px; font-weight: 900;'>5<span style='color:#3b82f6;'>+</span> 年</div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # --- 上2區塊：核心痛點引言 (滿版顯示) ---
        st.markdown(
            """
                        <div style="background-color: rgba(254, 242, 242, 0.9); padding: 20px; border-radius: 12px; border: 1px solid #fecaca; text-align: left; margin-bottom: 30px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);">
                            <div style="font-size: 1.15rem; color: #b91c1c; font-weight: bold; margin-bottom: 10px;">
                                「外媒直指臺灣街頭是行人地獄，我們的夜市真的安全嗎？」
                            </div>
                            <div style="color: #475569; font-size: 1rem; line-height: 1.7;">
                                根據統計，臺灣行人交通事故死亡率高達每十萬人 <b>1.56</b> 人，是鄰國日本的兩倍之多。而高達 <b>83.84%</b> 的國際旅客來臺必訪夜市。在人車混流、動線擁擠的環境中，過馬路不該是賭命！
                            </div>
                        </div>
                    """,
            unsafe_allow_html=True,
        )

        # --- 中1區塊：受眾價值卡片 (4 欄並排) ---
        st.markdown(
            "<h3 style='text-align: left; color: #1e293b; margin-bottom: 20px;'>🎯 這套系統能為您做什麼？</h3>",
            unsafe_allow_html=True,
        )

        c1, c2, c3 = st.columns(3, gap="small")
        with c3:
            with st.container(border=True):
                st.markdown("<h4>🏡 在地居民</h4>", unsafe_allow_html=True)
                st.write(
                    "比較居住區域的歷年交通趨勢，識別家門口的危險因子，精準規劃出低風險生活圈。"
                )
        with c2:
            with st.container(border=True):
                st.markdown("<h4>🏪 夜市攤商、行人</h4>", unsafe_allow_html=True)
                st.write(
                    "診斷特定夜市的交通環境風險，提早防範塞車與事故發生，保障客流與日常進出貨安全。"
                )
        with c1:
            with st.container(border=True):
                st.markdown("<h4>🏛️ 地方政府</h4>", unsafe_allow_html=True)
                st.write(
                    "透過全台對標與 YoY 數據比較，科學化評估「人本交通」政策執行成效與未來改善空間。"
                )

        # --- 中2層區塊：數據解密卡片 (3 欄並排) ---
        st.markdown(
            "<h3 style='color: #1e293b; margin-bottom: 15px;'><span style='font-size: 1.4rem;'>🧭</span> 車禍與夜市關聯性分析</h3>",
            unsafe_allow_html=True,
        )
        row1_col1, row1_col2, row1_col3 = st.columns(3, gap="small")

        with row1_col1:
            with st.container(border=True):
                st.markdown(
                    "<div class='action-title'>🔥 哪裡最危險？</div><div class='action-desc'>巨觀掃描全台事故熱區，找出版圖盲區。</div>",
                    unsafe_allow_html=True,
                )
                if st.button("🗺️ 探索全台夜市總體檢", use_container_width=True):
                    st.switch_page(
                        "pages/v_act1_all_accident.py"
                    )  # Navigates to another Streamlit page file.
        with row1_col2:
            with st.container(border=True):
                st.markdown(
                    "<div class='action-title'>🏙️ 我們的城市及格嗎？</div><div class='action-desc'>檢視跨縣市安全對標與年度進步率。<br><br></div>",
                    unsafe_allow_html=True,
                )
                if st.button("📊 縣市安全對標與趨勢", use_container_width=True):
                    st.switch_page("pages/v_act1_city_accident.py")
        with row1_col3:
            with st.container(border=True):
                st.markdown(
                    "<div class='action-title'>🔍 為何會發生？</div><div class='action-desc'>深入單一夜市，探究天氣、時段與肇因特徵。</div>",
                    unsafe_allow_html=True,
                )
                if st.button("🤖 單一夜市 AI 深度診斷", use_container_width=True):
                    st.switch_page("pages/v_act1_single_accident.py")

        st.markdown("<br>", unsafe_allow_html=True)

        # --- 中3層區塊：化數據為行動 (3 欄並排) ---
        st.markdown(
            "<h3 style='color: #1e293b; margin-bottom: 15px;'><span style='font-size: 1.4rem;'>🛡️</span>無標的夜市下之車禍分析</h3>",
            unsafe_allow_html=True,
        )

        with st.container(border=True, width=285):
            st.markdown(
                "<div class='action-title'>🏛️ 政策監督</div><div class='action-desc'>「停讓行人」新法真的有用嗎？檢視修法前後事故變化。</div>",
                unsafe_allow_html=True,
            )

            if st.button(
                "📈 政策成效歷史數據",
                type="secondary",
                use_container_width=True,
            ):
                st.switch_page("pages/v_act2_tableau.py")


if __name__ == "__main__":
    main()
