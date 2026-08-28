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

    # 建立一個通用的 HTML 產生器，利用 <object> 標籤與 Tableau 的 JavaScript API (viz_v1.js)，將公開的 Tableau Dashboard 嵌入到 Streamlit 中
    # 傳入不同的 url_path 即可共用同一段嵌入代碼
    def get_tableau_html(url_path, static_image):
        """組出嵌入單一 Tableau 儀表板所需的 HTML。

        三個分頁共用同一段嵌入程式碼，只有儀表板路徑與預覽圖不同。

        Args:
            url_path (str): Tableau Public 上的儀表板路徑，例如 `"shared/JJ6HP2KN6"`。
            static_image (str): 載入前顯示的預覽圖網址。

        Returns:
            str: 可交給 `components.html()` 渲染的 HTML 字串。
        """
        return f"""
        <div class='chart-container'>
            <div class='tableauPlaceholder' style='position: relative; width: 100%; height: 850px;'>
                <object class='tableauViz' style='display:none; width: 100%; height: 100%;'>
                    <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' />
                    <param name='embed_code_version' value='3' />
                    <param name='path' value='{url_path}' />
                    <param name='toolbar' value='no' />
                    <param name='static_image' value='{static_image}' />
                    <param name='animate_transition' value='yes' />
                    <param name='display_static_image' value='no' />
                    <param name='display_spinner' value='yes' />
                    <param name='display_overlay' value='yes' />
                    <param name='display_count' value='yes' />
                    <param name='language' value='zh-TW' />
                    <param name='filter' value=':original_view=yes' />
                </object>
            </div>
        </div>
        <script type='text/javascript'>
            var divElements = document.getElementsByClassName('tableauPlaceholder');
            var divElement = divElements[divElements.length - 1];
            var vizElement = divElement.getElementsByTagName('object')[0];
            vizElement.style.width = '100%';
            /* 強制設定為 850px 避免高度塌陷擠壓 */
            vizElement.style.height = '850px';
            var scriptElement = document.createElement('script');
            scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
            vizElement.parentNode.insertBefore(scriptElement, vizElement);
        </script>
        """

    tab1, tab2, tab3 = st.tabs(["政策有效嗎 ？", "車禍趨勢", "車禍肇因"])

    # 透過 components.html 將產生的語法渲染在畫面上，scrolling=False 隱藏預設捲軸以求美觀
    with tab1:
        html1 = get_tableau_html(
            "shared&#47;G4WQGYSTG",
            "https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;G4&#47;G4WQGYSTG&#47;1.png",
        )
        components.html(html1, height=920, scrolling=False)

    with tab2:
        html2 = get_tableau_html(
            "shared&#47;56837R6KD",
            "https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;56&#47;56837R6KD&#47;1.png",
        )
        components.html(html2, height=920, scrolling=False)

    with tab3:
        html3 = get_tableau_html(
            "views&#47;tjr104_mart&#47;2?:language=zh-TW&amp;:embed=true&amp;:sid=&amp;:redirect=auth",
            "https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;tj&#47;tjr104_mart&#47;2&#47;1.png",
        )
        components.html(html3, height=920, scrolling=False)


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
