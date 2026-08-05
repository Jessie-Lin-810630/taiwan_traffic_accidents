import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import src.task.core.c_data_service as ds
import src.task.core.c_ui as ui
from src.util.logger_crtx import get_logger
from src.util.redis_utils import get_cache

logger = get_logger(__name__)

st.set_page_config(layout="wide", page_title="各縣市夜市事故比較分析", page_icon="🏙️")

# 自定義地理排序清單
# 將縣市與區域陣列寫死，確保在圖表下拉選單與表格繪製時，能維持習慣的地理順序
# 修改處：將東部與東部離島合併為單一區域
REGION_ORDER = ["北部", "中部", "南部", "東部與東部離島", "離島"]
CITY_ORDER = [
    "臺北市", "新北市","基隆市", "桃園市", "新竹市", "新竹縣", "宜蘭縣",
    "苗栗縣", "臺中市", "彰化縣", "南投縣", "雲林縣",
    "嘉義市", "嘉義縣", "臺南市", "高雄市", "屏東縣",
    "花蓮縣", "臺東縣",
    "澎湖縣", "金門縣", "連江縣"]

# ==========================================
# 資料存取層
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def get_real_city_data():
    try:
        df = get_cache("market:national_master_df")
        if df is not None and not df.empty:
            df = df.copy()

            # 【去重複機制】防止跨夜市重疊區域的事故被重複計算
            if 'accident_id' in df.columns:
                df = df.drop_duplicates(subset=['accident_id'])

            df['accident_date'] = pd.to_datetime(df['accident_date'])
            df['year_quarter'] = df['accident_date'].dt.year.astype(str) + " Q" + df['accident_date'].dt.quarter.astype(str)
            df['city'] = df['nightmarket_city']
            
            df = df[(df['city'].notna()) & (df['city'].astype(str).str.strip() != '') & (df['city'] != 'None')]
            
            # 將花東及外島皆對應到「東部與東部離島」
            region_map = {
                "臺北市": "北部", "新北市": "北部", "基隆市": "北部", "桃園市": "北部", "新竹市": "北部", "新竹縣": "北部", "宜蘭縣": "北部",
                "臺中市": "中部", "苗栗縣": "中部", "彰化縣": "中部", "南投縣": "中部", "雲林縣": "中部",
                "臺南市": "南部", "高雄市": "南部", "嘉義市": "南部", "嘉義縣": "南部", "屏東縣": "南部",
                "花蓮縣": "東部與東部離島", "臺東縣": "東部與東部離島", 
                "澎湖縣": "離島", "金門縣": "離島", "連江縣": "離島"
            }
            df['region'] = df['city'].map(region_map).fillna("其他")
            
            # 加入附屬離島（蘭嶼、綠島、小琉球）的獨立區域劃分邏輯，將其歸入「東部與東部離島」
            # 夜市主檔的行政區欄位是 area_road（不是 AdminDistrict），比照 c_data_service 用關鍵字比對
            df_market = ds.get_all_nightmarkets()
            if not df_market.empty and 'area_road' in df_market.columns:
                admin_map = df_market.set_index('nightmarket_name')['area_road'].to_dict()
                df['area_road'] = df['nightmarket_name'].map(admin_map)
                mask_islands = df['area_road'].str.contains('琉球|蘭嶼|綠島', na=False)
                df.loc[mask_islands, 'region'] = '東部與東部離島'


            return df
    except Exception as e:
        # 前端不再往上拋，例外在此停止傳播，故帶 exc_info=True（ADR-0003）
        logger.error(f"讀取 market:national_master_df 失敗: {e}", exc_info=True)
        st.error(f"Redis 讀取失敗: {e}")
    return pd.DataFrame()

# 動態風險評級函數
# 依據傳入的數值與區域基準值(benchmark) 計算落差比例
def get_risk_level(val, benchmark):
    """依據與基準值的差距，給予分級標籤"""
    if benchmark == 0 or pd.isna(benchmark) or pd.isna(val): 
        return "⚪ 無資料", "#94a3b8"
    ratio = (val - benchmark) / benchmark
    if ratio > 0.3: return "🔴 極危險", "#ef4444"
    elif ratio > 0.15: return "🟠 危險", "#f97316"
    elif ratio > 0.05: return "🟡 注意", "#eab308"
    elif ratio >= -0.05: return "🔵 基準水準", "#3b82f6"
    else: return "🟢 安全", "#10b981"

def main():
    df_market = ds.get_all_nightmarkets()
    ui.render_sidebar(df_market)
    
    st.markdown("""
    <style>
    div[data-testid="stVerticalBlock"] > div { padding-bottom: 0rem; }
    .kpi-box { background-color: #ffffff; padding: 15px; border-radius: 8px; text-align: center; border: 1px solid #e2e8f0; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
    .kpi-title { font-size: 13px; color: #64748b; margin-bottom: 6px; font-weight: bold; }
    .kpi-value { font-size: 26px; font-weight: 900; color: #1e293b; margin-bottom: 4px; }
    .kpi-delta { font-size: 12.5px; font-weight: bold; padding: 2px 6px; border-radius: 4px; display: inline-block; margin-top: 4px; }
    .delta-good { color: #10b981; background-color: #d1fae5; }
    .delta-bad { color: #ef4444; background-color: #fee2e2; }
    .delta-neutral { color: #64748b; background-color: #f1f5f9; }
    .section-title { font-size: 15px; font-weight: bold; color: #1f2937; margin-bottom: 10px; margin-top: 15px; border-left: 4px solid #3b82f6; padding-left: 8px;}
    .metric-card { padding: 16px; border-radius: 8px; color: white; margin-bottom: 10px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
    .metric-card.danger { background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); }
    .metric-card.neutral { background: linear-gradient(135deg, #64748b 0%, #475569 100%); }
    .metric-card.safe { background: linear-gradient(135deg, #10b981 0%, #059669 100%); }
    .metric-card-title { font-size: 13px; opacity: 0.9; margin-bottom: 4px; font-weight: bold; }
    .metric-card-entity { font-size: 24px; font-weight: bold; margin-bottom: 4px; letter-spacing: 1px; }
    .metric-card-value { font-size: 15px; font-weight: 500; background-color: rgba(255,255,255,0.2); display: inline-block; padding: 2px 8px; border-radius: 4px; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h2 style='margin-top: 0px; margin-bottom: 5px;'>🏙️ 各縣市夜市事故比較分析：<span style='color:#3b82f6;'>城市安全對標</span></h2>", unsafe_allow_html=True)
    st.info("""
    💡 數據判讀須知：
    1. 空間範圍：本頁指標與排名【包含夜市方圓 500 公尺核心區內所有類型車禍】，並已剔除重疊事故紀錄。
    2. 時間完整性：⚠️ 2026 Q3 數據目前僅統計至 7 月底。季度比較趨勢之落差係因資料統計週期不完整所致，非實際事故量大幅下降，判讀時請留意。
    """)
    
    df_raw = get_real_city_data()
    if df_raw.empty:
        st.warning("無數據可供分析。")
        return

    # 切割為雙欄寬版配置
    col_left, col_right = st.columns([0.7, 2.3], gap="large")

    with col_left:
        st.markdown('<div class="section-title" style="margin-top:0px;">切換分析視角</div>', unsafe_allow_html=True)
        mode = st.radio(
            "切換分析視角", 
            ["綜合危險指數 (PDI)", "總事故件數", "死亡人數", "受傷人數"], 
            horizontal=True, 
            label_visibility="collapsed"
        )
        
        is_pdi_mode = (mode == "綜合危險指數 (PDI)")
        if is_pdi_mode:
            agg_col, agg_func, unit, fmt = 'pdi_score', 'mean', '分', '{:.2f}'
        elif mode == "總事故件數":
            agg_col, agg_func, unit, fmt = 'accident_id', 'count', '件', '{:,.0f}'
        elif mode == "死亡人數":
            agg_col, agg_func, unit, fmt = 'death_count', 'sum', '人', '{:,.0f}'
        else:
            agg_col, agg_func, unit, fmt = 'injury_count', 'sum', '人', '{:,.0f}'

        st.markdown("""
        <div style="background-color: #f8fafc; border-left: 4px solid #94a3b8; padding: 12px 16px; border-radius: 0 8px 8px 0; margin-bottom: 20px; margin-top: 10px;">
            <div style="font-weight: bold; color: #334155; margin-bottom: 6px; font-size: 13px;">💡 什麼是 PDI 危險指數？</div>
            <ul style="font-size: 12.5px; color: #475569; line-height: 1.6; margin-bottom: 0; padding-left: 18px;">
                <li>PDI公式： ((死亡×10 + 受傷×2) / 該區總事故數) × 1.5</li>
                <li>分母對齊： 除以總件數標準化風險，消除規模誤差。</li>
                <li>判定： 數值越高代表一旦發生事故「非死即傷」機率越高。</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="section-title">🗺️ 分析區域與縣市篩選</div>', unsafe_allow_html=True)
        with st.container(border=True):
            # 強制地理排序
            raw_regions = df_raw['region'].unique()
            region_opts = ["全部區域"] + [r for r in REGION_ORDER if r in raw_regions]
            sel_region = st.selectbox("分析區域", region_opts)
            
            if sel_region != "全部區域":
                raw_cities = df_raw[df_raw['region'] == sel_region]['city'].unique()
            else:
                raw_cities = df_raw['city'].unique()
                
            city_opts_list = [c for c in CITY_ORDER if c in raw_cities]
            sel_city = st.selectbox("分析縣市", ["全部縣市"] + city_opts_list)

        st.markdown('<div class="section-title">📅 分析時間篩選</div>', unsafe_allow_html=True)
       
        with st.container(border=True):
            years_available = sorted(list(df_raw['Year'].unique()), reverse=True)
            sel_year_str = st.selectbox("年份", ["全部年份"] + [str(y) for y in years_available])
            
            c_q, c_m = st.columns(2)
            with c_q: sel_q = st.selectbox("季度", ["全年", "第 1 季", "第 2 季", "第 3 季", "第 4 季"])
            with c_m: sel_m = st.selectbox("月份", ["全部"] + [f"{i} 月" for i in range(1, 13)])
            
            # 總表的 Weekday 存的是 dim_accident_day 的中文字串（星期一…星期日），
            # 篩選值必須是同型別，用數字比對會靜默得到空集合
            week_map = {
                "全部": None,
                "週一": "星期一", "週二": "星期二", "週三": "星期三", "週四": "星期四",
                "週五": "星期五", "週六": "星期六", "週日": "星期日",
            }
            sel_w = st.selectbox("星期", list(week_map.keys()))
            
            heat_mode = st.radio("時段", ["全部", "白天 (06-18)", "夜間 (18-06)"], horizontal=True)

        # ------------------
        # 資料過濾邏輯
        # ------------------
        # 動態決定群組維度
        # 如果使用者沒有鎖定單一縣市，圖表就以「縣市(city)」為單位作比較；如果鎖定了特定縣市，圖表會自動往下取，改以該縣市內的「夜市(nightmarket_name)」為單位作比較
        target_entity_col = 'city' if sel_city == "全部縣市" else 'nightmarket_name'
        display_area_name = sel_city if sel_city != "全部縣市" else (sel_region if sel_region != "全部區域" else "全台")
        entity_label = "選擇的縣市" if target_entity_col == 'city' else "左側選單選擇的夜市"

        df_time_base = df_raw.copy()
        if sel_q != "全年": df_time_base = df_time_base[df_time_base['Quarter'] == int(sel_q.split()[1])]
        if sel_m != "全部": df_time_base = df_time_base[df_time_base['Month'] == int(sel_m.split()[0])]
        if sel_w != "全部": df_time_base = df_time_base[df_time_base['Weekday'] == week_map[sel_w]]
        if "白天" in heat_mode: df_time_base = df_time_base[(df_time_base['Hour'] >= 6) & (df_time_base['Hour'] < 18)]
        elif "夜間" in heat_mode: df_time_base = df_time_base[(df_time_base['Hour'] >= 18) | (df_time_base['Hour'] < 6)]

        df_area_base = df_time_base.copy()
        if sel_region != "全部區域": df_area_base = df_area_base[df_area_base['region'] == sel_region]
        if sel_city != "全部縣市": df_area_base = df_area_base[df_area_base['city'] == sel_city]

        df_current = df_area_base.copy()
        if sel_year_str != "全部年份":
            df_current = df_current[df_current['Year'] == int(sel_year_str)]

    with col_right:
        st.markdown(f"<div style='font-size:17px; font-weight:bold; color:#1f2937; margin-top:5px; margin-bottom:12px;'>🚦 {display_area_name} 安全基準線與現況排名</div>", unsafe_allow_html=True)
        
        # 全國同期母體，供下方體檢表的國均對標使用
        df_nat_current = df_time_base.copy()
        if sel_year_str != "全部年份":
            df_nat_current = df_nat_current[df_nat_current['Year'] == int(sel_year_str)]

        # ----------------------------------------
        # 綜合安全體檢表 - 指標計算與國均校正
        # ----------------------------------------
        current_val = df_current[agg_col].agg(agg_func) if not df_current.empty else 0
        
        if is_pdi_mode:
            sub_col, sub_func = 'accident_id', 'count'
            sub_val = len(df_current)
            sub_title, sub_unit, sub_fmt = "事故總件數", "件", "{:,.0f}"
            sub_is_pdi = False
        else:
            sub_col, sub_func = 'pdi_score', 'mean'
            sub_val = df_current['pdi_score'].mean() if not df_current.empty else 0
            sub_title, sub_unit, sub_fmt = "平均 PDI", "分", "{:.2f}"
            sub_is_pdi = True

        # 校正體檢表的比較基準。若選擇的是單一縣市，體檢表應對標「全國縣市平均」；若為區域，應對標「全國區域平均」。
        if sel_city != "全部縣市":
            kpi_benchmark = df_nat_current.groupby('city')[agg_col].agg(agg_func).mean() if not df_nat_current.empty else 0
            sub_benchmark = df_nat_current.groupby('city')[sub_col].agg(sub_func).mean() if not df_nat_current.empty else 0
            kpi_entity_name = "縣市"
        elif sel_region != "全部區域":
            kpi_benchmark = df_nat_current.groupby('region')[agg_col].agg(agg_func).mean() if not df_nat_current.empty else 0
            sub_benchmark = df_nat_current.groupby('region')[sub_col].agg(sub_func).mean() if not df_nat_current.empty else 0
            kpi_entity_name = "區域"
        else:
            kpi_benchmark = 0
            sub_benchmark = 0

        def get_kpi_subtitle(val, benchmark, is_pdi, c_unit):
            if benchmark == 0 or pd.isna(benchmark): return "無基準資料"
            diff = val - benchmark
            pct = (diff / benchmark) * 100 if benchmark > 0 else 0
            fmt_diff = f"{diff:+.2f}" if is_pdi else f"{diff:+,.0f}"
            fmt_bench = f"{benchmark:.2f}" if is_pdi else f"{benchmark:,.0f}"
            return f"{fmt_diff} ({pct:+.1f}%)<br>國均{fmt_bench} {c_unit}"

        yoy_val, yoy_text, yoy_class = 0, "-", "delta-neutral"
        prev_year = None
        if sel_year_str != "全部年份":
            prev_year = int(sel_year_str) - 1
            df_prev = df_area_base[df_area_base['Year'] == prev_year]
            if not df_prev.empty:
                prev_val = df_prev[agg_col].agg(agg_func) if not df_prev.empty else 0
                if prev_val > 0:
                    yoy_val = ((current_val - prev_val) / prev_val) * 100
                    yoy_text = "↑ 惡化" if yoy_val > 0 else "↓ 進步" if yoy_val < 0 else "持平"
                    yoy_class = "delta-bad" if yoy_val > 0 else "delta-good" if yoy_val < 0 else "delta-neutral"
            else:
                prev_val = 0
                yoy_text = "無前年資料"

        with st.container(border=False):
            k1, k2, k3, k4 = st.columns(4)
            
            with k1:
                if kpi_benchmark > 0:
                    diff_text = get_kpi_subtitle(current_val, kpi_benchmark, is_pdi_mode, unit)
                    diff_class = "delta-bad" if (current_val - kpi_benchmark) > 0 else "delta-good" if (current_val - kpi_benchmark) < 0 else "delta-neutral"
                else:
                    diff_text = "全台總計<br>(無對標)"
                    diff_class = "delta-neutral"
                
                st.markdown(f"""
                <div class='kpi-box'>
                    <div class='kpi-title'>{mode}</div>
                    <div class='kpi-value'>{fmt.format(current_val)} <span style='font-size:14px; font-weight:normal;'>{unit}</span></div>
                    <div class='kpi-delta {diff_class}'>{diff_text}</div>
                </div>""", unsafe_allow_html=True)
            
            with k2:
                level_text, level_color = get_risk_level(current_val, kpi_benchmark) if kpi_benchmark > 0 else ("⚪ 基準", "#94a3b8")
                st.markdown(f"""
                <div class='kpi-box'>
                    <div class='kpi-title'>對標評級判定</div>
                    <div class='kpi-value' style='color:{level_color};'>{level_text}</div>
                    <div class='kpi-delta delta-neutral'>依據:<br>與國均差距</div>
                </div>""", unsafe_allow_html=True)
            
            with k3:
                if sub_benchmark > 0:
                    sub_diff_text = get_kpi_subtitle(sub_val, sub_benchmark, sub_is_pdi, sub_unit)
                    sub_diff_class = "delta-bad" if (sub_val - sub_benchmark) > 0 else "delta-good" if (sub_val - sub_benchmark) < 0 else "delta-neutral"
                else:
                    sub_diff_text = "全台總計<br>(無對標)"
                    sub_diff_class = "delta-neutral"

                st.markdown(f"""
                <div class='kpi-box'>
                    <div class='kpi-title'>{sub_title}</div>
                    <div class='kpi-value'>{sub_fmt.format(sub_val)} <span style='font-size:14px; font-weight:normal;'>{sub_unit}</span></div>
                    <div class='kpi-delta {sub_diff_class}'>{sub_diff_text}</div>
                </div>""", unsafe_allow_html=True)
            
            with k4:
                if sel_year_str != "全部年份" and prev_year is not None and prev_val > 0:
                    kpi_title_4 = f"YoY 變化 ({sel_year_str} vs {prev_year})"
                    display_yoy = f"{abs(yoy_val):.2f}%" if yoy_val != 0 else "-"
                    diff_abs = current_val - prev_val
                    fmt_diff = f"{diff_abs:+.2f}" if is_pdi_mode else f"{diff_abs:+,.0f}"
                    fmt_val = "{:.2f}" if is_pdi_mode else "{:,.0f}"
                    
                    yoy_sub = f"<br><span style='font-weight:normal; font-size:11.5px;'>{int(sel_year_str) - int(prev_year)}年淨變化:{fmt_diff} {unit}</span>"
                else:
                    kpi_title_4 = "YoY 總體變化率"
                    display_yoy = "-"
                    yoy_sub = "<span style='font-weight:normal; font-size:11.5px;'>請選擇單一年份以啟用 YoY 對比</span>"

                st.markdown(f"""
                <div class='kpi-box'>
                    <div class='kpi-title'>{kpi_title_4}</div>
                    <div class='kpi-value'>{display_yoy}</div>
                    <div class='kpi-delta {yoy_class}'>{yoy_text}{yoy_sub}</div>
                </div>""", unsafe_allow_html=True)

        # 歷年趨勢與細部排名並排：左看時間軸的走勢，右看當期各單位的名次
        col_t_left, col_t_right = st.columns([1.4, 1], gap="medium")

        with col_t_left:
            st.markdown(f'<div class="section-title">📈 歷年趨勢 ({display_area_name})</div>', unsafe_allow_html=True)
            with st.container(border=True):
                if not df_area_base.empty:
                    # Plotly 動態圖表切換
                    # 若為 PDI 指數，使用「折線圖 (Lines)」顯示波動，並加上全台均線對標；若為件數或人數，改用「長條圖 (Bar)」表現累積量體
                    trend_agg = df_area_base.groupby('Year').agg(val=(agg_col, agg_func)).reset_index().sort_values('Year')
                    nat_trend_agg = df_time_base.groupby('Year').agg(val=(agg_col, agg_func)).reset_index().sort_values('Year')

                    fig_trend = go.Figure()
                    if is_pdi_mode:
                        fig_trend.add_trace(go.Scatter(x=trend_agg['Year'], y=trend_agg['val'], name=f'{display_area_name} PDI', mode='lines+markers', line=dict(color='#ef4444', width=3)))
                        fig_trend.add_trace(go.Scatter(x=nat_trend_agg['Year'], y=nat_trend_agg['val'], name='全台均線', mode='lines', line=dict(color='#64748b', width=2, dash='dash')))
                        fig_trend.update_yaxes(title="平均 PDI")
                    else:
                        fig_trend.add_trace(go.Bar(x=trend_agg['Year'], y=trend_agg['val'], name=f'{display_area_name} {mode}', marker_color='#3b82f6'))
                        fig_trend.update_yaxes(title=mode)

                    fig_trend.update_layout(xaxis=dict(type='category'), legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1), margin=dict(l=0, r=0, t=10, b=0), height=320, plot_bgcolor='white', hovermode="x unified")
                    st.plotly_chart(fig_trend, use_container_width=True)
                else:
                    st.info("無趨勢資料")

        with col_t_right:
            st.markdown(f'<div class="section-title">🏅 細部排名 ({display_area_name})</div>', unsafe_allow_html=True)
            with st.container(border=True):
                # 長表：一列一個縣市／夜市，欄位為名次與指標值
                entity_metrics = (
                    df_current.groupby(target_entity_col).agg(val=(agg_col, agg_func)).reset_index()
                    if not df_current.empty else pd.DataFrame()
                )

                if not entity_metrics.empty:
                    entity_stats = entity_metrics.sort_values(by='val', ascending=False).reset_index(drop=True)

                    # 使用 rank(method='min') 自動處理同數值同名次的狀況
                    entity_stats['排名'] = entity_stats['val'].rank(ascending=False, method='min').astype(int)
                    entity_stats[mode] = entity_stats['val'].apply(lambda x: fmt.format(x))

                    df_rank_long = entity_stats[['排名', target_entity_col, mode]].rename(columns={target_entity_col: entity_label})
                    st.dataframe(df_rank_long, use_container_width=True, hide_index=True, height=320)
                else:
                    st.info(f"目前篩選條件下無 {display_area_name} 的資料。")

        st.markdown(f'<div class="section-title">🎯 {display_area_name} 安全風險象限分析 (維度：{entity_label})</div>', unsafe_allow_html=True)
        with st.container(border=True):
            # 象限分析
            # 結合件數與平均 PDI 兩個維度，利用 Plotly 繪製散佈圖
            if not df_current.empty:
                quadrant_stats = df_current.groupby(target_entity_col).agg(
                    pdi_mean=('pdi_score', 'mean'), acc_count=('accident_id', 'count')).reset_index()

                if len(quadrant_stats) > 1:
                    avg_pdi_line = quadrant_stats['pdi_mean'].mean()
                    avg_count_line = quadrant_stats['acc_count'].mean()

                    fig_scatter = px.scatter(
                        quadrant_stats, x="acc_count", y="pdi_mean", text=target_entity_col,
                        color="pdi_mean", color_continuous_scale="Reds",
                        labels={"acc_count": "事故發生件數", "pdi_mean": "平均危險指數 (PDI)"})

                    # 加入平均輔助線
                    fig_scatter.add_hline(y=avg_pdi_line, line_dash="dot", line_color="#94a3b8", annotation_text="PDI 均線")
                    fig_scatter.add_vline(x=avg_count_line, line_dash="dot", line_color="#94a3b8", annotation_text="件數均線")
                    fig_scatter.update_traces(textposition='top center', marker=dict(size=12, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
                    fig_scatter.update_layout(height=380, margin=dict(l=10, r=10, t=10, b=10), coloraxis_showscale=False, plot_bgcolor='#f8fafc')
                    st.plotly_chart(fig_scatter, use_container_width=True)
                else:
                    st.info(f"當前範圍內需有 2 個以上的 {entity_label} 方可繪製分佈象限圖。")
            else:
                st.info("無足夠資料點繪製象限圖")

        suffix = f"({sel_year_str} vs {prev_year})" if prev_year else "(請選擇單一年份)"
        st.markdown(f'<div class="section-title">⚖️ 各 {entity_label} YoY 改善幅度排行總表 <span style="font-size:13px; font-weight:normal; color:#64748b;">{suffix}</span></div>', unsafe_allow_html=True)
        st.markdown("""
        <div style="background-color: #f8fafc; border-left: 4px solid #f8fafc; padding: 10px; border-radius: 0 8px 8px 0; margin-bottom: 12px; font-size: 13px; color: #334155; line-height: 1.5;">
            <b>💡 YoY 變化量判讀差異：</b><br>
            • 此圖表為「今年對比去年」的變化量，排在越前面代表狀況惡化最嚴重。<br>
            • 正值 (紅色) 代表「變危險/增加」；負值 (綠色) 代表「變安全/減少」。<br>
            • 指標若選擇 PDI 則顯示改善率 (%)，其餘絕對指標顯示淨變化量以避免基數扭曲。
        </div>
        """, unsafe_allow_html=True)
        
        with st.container(border=True):
            if sel_year_str == "全部年份":
                st.info("請於左側選擇單一年份以產生對比分析。")
            else:
                yoy_list = []
                if not df_current.empty and not df_prev.empty:
                    curr_stats = df_current.groupby(target_entity_col).agg(val=(agg_col, agg_func)).reset_index()
                    prev_stats = df_prev.groupby(target_entity_col).agg(val=(agg_col, agg_func)).reset_index()
                    
                    for _, row in curr_stats.iterrows():
                        entity_name = row[target_entity_col]
                        curr_val = row['val']
                        prev_row = prev_stats[prev_stats[target_entity_col] == entity_name]
                        
                        if not prev_row.empty:
                            prev_val = prev_row.iloc[0]['val']
                            if prev_val > 0:
                                delta_pct = ((curr_val - prev_val) / prev_val) * 100
                                delta_abs = curr_val - prev_val
                                yoy_list.append({'名稱': entity_name, 'YoY': delta_pct, '淨變化': delta_abs})
                
                if yoy_list:
                    yoy_df = pd.DataFrame(yoy_list).sort_values('YoY', ascending=False).reset_index(drop=True)
                    yoy_df['排名'] = range(1, len(yoy_df) + 1)
                    
                    if is_pdi_mode:
                        yoy_df['YoY_str'] = yoy_df['YoY'].apply(lambda x: f"+{x:.2f}%" if x > 0 else f"{x:.2f}%")
                        row_label = 'YoY 改善率 (%)'
                    else:
                        yoy_df['YoY_str'] = yoy_df.apply(lambda row: f"+{row['淨變化']:,.0f} ({row['YoY']:+.2f}%)" if row['淨變化'] > 0 else f"{row['淨變化']:,.0f} ({row['YoY']:+.2f}%)", axis=1)
                        row_label = 'YoY 淨變化'
                        
                    yoy_df['排名_str'] = yoy_df['排名'].astype(str)
                    df_yoy_transposed = yoy_df[['名稱', '排名_str', 'YoY_str']].set_index('名稱').T
                    df_yoy_transposed.index = ['排名', row_label]
                    
                    def color_yoy_row(row):
                        if row.name == row_label:
                            return ['color: #ef4444; font-weight: bold;' if '+' in str(v) else 'color: #10b981; font-weight: bold;' if '-' in str(v) else '' for v in row]
                        return ['' for _ in row]

                    st.dataframe(df_yoy_transposed.style.apply(color_yoy_row, axis=1), use_container_width=True)
                else:
                    st.info("無足夠的前期資料可供比對計算。")

if __name__ == "__main__":
    main()