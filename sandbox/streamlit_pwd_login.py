import streamlit as st
import os
from dotenv import load_dotenv


def initialize_login():
    load_dotenv()
    if "true_pwd" not in st.session_state:
        st.session_state.true_pwd = os.getenv("STREAMLIT_PASSWORD", "")
    if "fail_counts" not in st.session_state:
        st.session_state.fail_counts = 0
    if "authed" not in st.session_state:
        st.session_state.authed = False


def check_pwd():
    # 已達 5 次錯誤，直接終止
    if st.session_state.fail_counts >= 5:
        st.write("密碼錯誤，累積已達錯誤5次，終止服務")
        st.stop()

    true_pwd = st.session_state.true_pwd
    pwd = st.session_state.password  # 由 text_input 的 key 帶入

    if pwd == true_pwd:
        st.session_state.authed = True
    else:
        st.session_state.fail_counts += 1
        if st.session_state.fail_counts >= 5:
            st.write("密碼錯誤，累積已達錯誤5次，終止服務")
            st.stop()
        else:
            st.warning(f"密碼錯誤，累積錯誤 {st.session_state.fail_counts} 次")


initialize_login()

if st.session_state.authed:
    st.write("Hello!")
else:
    st.text_input(
        label="請輸入密碼：",
        max_chars=50,
        key="password",
        type="password",
        autocomplete="off",
        on_change=check_pwd,
        placeholder="請使用阿拉伯數字、大寫英文、小寫英文，共計最多50個字",
    )
