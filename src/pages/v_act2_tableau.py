"""Streamlit 分頁：修法前後分析研究，嵌入三張 Tableau Public 儀表板。

主題是 2023 年 6 月 30 日《道路交通管理處罰條例》修法前後的事故變化。本頁不做
任何運算，圖表都由 Tableau Public 提供，這裡只負責嵌入與版面樣式。
"""

import streamlit as st
import streamlit.components.v1 as components

import src.task.core.c_ui as ui
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

st.set_page_config(layout="wide", page_title="修法前後分析研究", page_icon="🖼️")

# Tableau Public 上這頁三張圖所屬的 workbook 名稱，三個分頁都取自同一本
TABLEAU_WORKBOOK = "tjr104_mart"

# 嵌入時的顯示參數，:device 固定成桌機版面避免窄視窗被切成手機版，
# :tabs 關掉 workbook 內部的分頁列，讓每個分頁只看得到自己那張圖
TABLEAU_EMBED_PARAMS = (
    ":embed=y"
    "&:showVizHome=no"
    "&:display_count=n"
    "&:language=zh-TW"
    "&:device=desktop"
    "&:tabs=no"
    "&:showShareOptions=false"
)


def get_tableau_html(sheet):
    """組出嵌入單一 Tableau 儀表板所需的 HTML。

    三個分頁共用同一段嵌入程式碼，只有 workbook 內的 sheet 編號不同。

    Args:
        sheet (str): workbook 內的 sheet 編號，例如 `"1"`。

    Returns:
        str: 可交給 `components.html()` 渲染的 HTML 字串。

    Notes:
        走 `views/<workbook>/<sheet>` 這條永久路徑，不能用 Share 產生的
        `shared/<token>` 短碼，因為後者會不定期失效。
    """
    src = (
        f"https://public.tableau.com/views/{TABLEAU_WORKBOOK}/{sheet}"
        f"?{TABLEAU_EMBED_PARAMS}"
    )
    return f"""
    <div class='chart-container'>
        <iframe
            src='{src}'
            width='100%'
            height='850'
            frameborder='0'
            scrolling='no'
            allowfullscreen
            allow='fullscreen'
            style='display: block; border: none;'>
        </iframe>
    </div>
    """


def act5_render():
    """畫出頁面樣式與三個嵌入 Tableau 儀表板的分頁標籤。

    分頁標籤的滑鼠停留與選取狀態都改成與整體系統一致的紅色主色調。
    """
    # 利用 data-baseweb="tab" 選擇器，修改分頁標籤 (Tabs) 的預設外觀，使 hover 與 active 狀態呈現紅色 (#E53935)，與整體系統主色調統一
    st.markdown(
        """
    <style>
        .block-container { padding-top: 4rem; }
        .title-banner {
            padding: 10px 0px;
            margin-bottom: 20px;
            text-align: left;
        }
        .title-banner h2 { margin: 0; color: #333333; font-weight: normal; font-size: 26px; }
        .chart-container {
            border: 2px solid #000000;
            border-radius: 12px;
            padding: 10px;
            background-color: #FFFFFF;
            margin-bottom: 10px;
        }

        /* 修改分頁標籤文字：加粗與稍微放大 */
        button[data-baseweb="tab"] p {
            font-weight: bold;
            font-size: 18px;
            color: #666666;
        }

        /* 滑鼠游標懸停時的顏色變化 */
        button[data-baseweb="tab"]:hover p {
            color: #E53935;
        }

        /* 選取狀態下的顏色變化與底線顏色 */
        button[data-baseweb="tab"][aria-selected="true"] p {
            color: #E53935 !important;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            border-bottom-color: #E53935 !important;
        }
    </style>
    <div class="title-banner">
        <h1>2023年06月30日《道路交通管理處罰條例》修法前後分析研究</h1>
    </div>
    """,
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3 = st.tabs(["政策有效嗎 ？", "車禍趨勢", "車禍肇因"])

    # sheet 編號對應 Tableau Public 上 tjr104_mart 這本 workbook 的分頁順序
    with tab1:
        components.html(get_tableau_html("1"), height=920, scrolling=False)

    with tab2:
        components.html(get_tableau_html("3"), height=920, scrolling=False)

    with tab3:
        components.html(get_tableau_html("2"), height=920, scrolling=False)


def main():
    """組出修法前後分析研究頁。

    取得夜市主檔只為了畫出側邊欄，本頁的圖表本身不依賴它。資料服務層故障時，
    前端是例外停止傳播之處，因此完整記錄後顯示錯誤訊息並中止本次渲染。

    Notes:
        前端負責決定如何降級，參考 ADR-0003。
    """
    ui.render_sidebar()
    act5_render()


if __name__ == "__main__":
    main()
