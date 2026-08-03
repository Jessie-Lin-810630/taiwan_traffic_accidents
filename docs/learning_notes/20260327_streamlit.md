# app.py
0. 本機測試streamlit，只需在terminal輸入
    ```
        streamlit run /.../.../app.py # 主程式入口

        # 查看網頁
        http://localhost:8501 #8501是預設端口
    ```
    `切記，streamlit run時，會以使用終端機的當前工作目錄作為根目錄。`
1.
```
# text-align: centered text
# padding: padding
# background: #ffffff 代表white background
# border-radius: 12px 代表rounded corners
# box-shadow: 設定元素的框架周圍添加陰影效果。
# border: 邊界
＃ action-title:自定義的 class 名稱，用於樣式化代表「動作」、「操作」或「功能」的標題元素。
# action-desc:定義內文樣式。


 st.markdown("""
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
                """, unsafe_allow_html=True)
```

2. col1, col2, .... = st.column() & with col1 & col1.methods:
    ```
        st.column() # 建立多欄式佈局，回傳多個可操作的 column 區塊 (tuple)。
    ```

3. variablesA = st.empty() & with variablesA.container() & variablesA.empty()

4. st.container() : 建立一個可包含其他元素的區塊 (靜態容器)。
    ```
        # 使用container物件有兩個方法
        # 方法一
        st.write("在容器外")
        container_object_variables = st.container()
        container_object-variables.write("在容器內")

        # 方法二
        st.write("在容器外)
        with st.container():
            st.write("在容器內")

    ```

5. st.empty(): 建立一個暫時的空白位置 (動態容器)，可動態更新與清除內容。

6. st.set_page_config()

7. st.button()
    ```
        st.button(label, use_container_width, on_click)
        # label: button顯示名稱。use_container_width: 是否讓按鈕寬度填滿整個容器，預設為 False。
        # on_click: 指定點選按鈕的話，要調用什麼函式
    ```
8. st.switch_page()
    ```
        st.switch_page("page file(another .py)")

    ```
9. st.radio()
    ```
        # 製作選項圓鈕，使用者點下的選項值會被回傳到變數容器中。
        mode = st.radio(label="切換分析視角",
                        options=["綜合危險指數 (PDI)", "事故總件數"],
                        horizontal=True,
                        label_visibility="collapsed")
        # label : 圓鈕識別名，會顯示於 radio 上方（必填）
        # options : 選項資料（list、tuple 或其他 iterable，必填）
        # horizontal :  設為 True 時選項會橫向排列，預設為垂直(False)
        # label_visibility : 是否要顯示label，預設是"visible"
    ```
