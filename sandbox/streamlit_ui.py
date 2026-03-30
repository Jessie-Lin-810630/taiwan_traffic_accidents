import streamlit as st


def html_template():
    """回傳無縮排的 HTML 卡片模板字串"""
    # CSS：使用 clamp() 函數達成響應式 (RWD) 字體大小
    # 讓卡片在手機版與電腦版螢幕上都能保持最佳排版比例
    # white-space:nowrap 確保夜市名稱太長時不斷行並以 "..."
    return """
<a href="{url}" target="_blank" style="text-decoration:none;display:inline-block;">
<div class="pdi-card" style="width:clamp(180px,30vw,260px);padding:15px;border-radius:12px;background-color:#ffffff;box-shadow:0 4px 10px rgba(0,0,0,0.15);text-align:center;border:1px solid #eee;transition:transform 0.15s ease, box-shadow 0.15s ease;cursor:pointer;">
<div style="font-size:clamp(16px,2vw,22px);font-weight:bold;margin-bottom:8px;color:#333;">🥇 第 {rank} 名</div>
<div style="font-size:clamp(14px,2vw,20px);font-weight:bold;color:#333;margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{name}</div>
<div style="font-size:clamp(12px,1.8vw,18px);margin-bottom:4px;color:#333;">PDI：<b>{pdi}</b>（{level}）</div>
<div style="font-size:clamp(12px,1.5vw,16px);color:#555;">事故數：{count} 件</div>
</div>
</a>
"""


# CSS：危險分級漸層色塊
# 使用 linear-gradient ，從綠(安全)、黃(注意)、橘(危險)到紅(極危險)
def danger_color(pdi):
    """根據 PDI 分數回傳對應的 CSS 漸層背景顏色"""
    if pdi <= 10:
        return "linear-gradient(135deg, #81c784, #43a047)"  # 綠
    elif pdi <= 30:
        return "linear-gradient(135deg, #fff176, #fdd835)"  # 黃
    elif pdi <= 60:
        return "linear-gradient(135deg, #ffcc80, #ff7043)"  # 橘
    else:
        return "linear-gradient(135deg, #ff8a80, #e53935)"  # 紅


def pdi_divider(level):
    """根據危險等級渲染 Streamlit 水平分隔線 (hr)"""
    colors = {
        "安全": "linear-gradient(90deg, #43a047, #81c784 )",
        "注意": "linear-gradient(90deg, #fff59d, #fdd835 )",
        "危險": "linear-gradient(90deg, #ff7043, #ffcc80 )",
        "極危險": "linear-gradient(90deg, #e53935, #ff8a80 )"
    }

    st.markdown(f"""
    <hr style="
        border: 0;
        height: 5px;
        background: {colors.get(level, '#eee')};
        border-radius: 3px;
    ">
    """, unsafe_allow_html=True)
