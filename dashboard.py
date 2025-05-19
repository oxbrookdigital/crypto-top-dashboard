# dashboard.py
import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone # Added timezone
import time # For sleep
import traceback # For detailed error logging in sidebar

# --- IMPORT YOUR MODULES ---
import thresholds_config as th 
import data_fetcher 
import formulas

DB_PATH = 'data/crypto_metrics.db'

st.set_page_config(layout="wide", page_title="Crypto Top Detection Dashboard")
st.title("📈 Crypto Market Cycle Top Detection Dashboard")
# Removed the timestamp from here as it might be confusing if data isn't fetched at this exact moment
# st.caption(f"Dashboard Last Refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (Note: Underlying data sources update at different intervals)")


# --- DATABASE UTILITIES ---
def fetch_from_db_dash(query): 
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Dashboard DB error: {e} for query: {query}")
        if "timestamp" in query.lower():
             return pd.DataFrame(columns=['timestamp'])
        elif "date" in query.lower():
             return pd.DataFrame(columns=['date'])
        return pd.DataFrame()
    finally:
        conn.close()
    
    if not df.empty:
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        elif 'date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['date']):
            try:
                df['date'] = pd.to_datetime(df['date']) 
            except Exception as e:
                print(f"Could not convert date column: {e} in query {query}")
                df['date'] = pd.NaT # Use NaT for failed conversions
    return df

# --- Risk Color Coding Function ---
def get_risk_color_html(value, high_threshold, medium_threshold, low_is_good=True, value_format=".2f"):
    color = "green" 
    risk_level = "Low"
    if value is None or pd.isna(value):
        return f"<span style='color:grey; font-weight:bold;'>Data N/A</span>"
    try:
        value_float = float(value)
        # Ensure thresholds are floats for comparison, handle None from config if it ever occurs
        high_threshold = float(high_threshold) if high_threshold is not None else (float('-inf') if not low_is_good else float('inf'))
        medium_threshold = float(medium_threshold) if medium_threshold is not None else (float('-inf') if not low_is_good else float('inf'))

    except ValueError:
        return f"<span style='color:grey; font-weight:bold;'>Invalid Value ({value})</span>"

    if low_is_good: 
        if value_float >= high_threshold:
            color = "red"; risk_level = "High"
        elif value_float >= medium_threshold:
            color = "orange"; risk_level = "Medium"
    else: # Lower values are riskier
        if value_float <= high_threshold: # For this logic, high_threshold is the "very low" point for red
            color = "red"; risk_level = "High (Risk)" 
        elif value_float <= medium_threshold:
            color = "orange"; risk_level = "Medium (Risk)"
            
    return f"<span style='color:{color}; font-weight:bold;'>{risk_level} ({value_float:{value_format}})</span>"

# --- FUNCTION TO TRIGGER DATA REFRESH ---
def trigger_data_update_and_calculations():
    """Calls all data fetching and formula calculation functions."""
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.sidebar.info(f"[{current_time_str}] Starting data update cycle...")

    # Step 1: Fetch new data from external APIs
    st.sidebar.text("Fetching API data...")
    data_fetcher.init_db() 
    data_fetcher.fetch_crypto_prices()
    data_fetcher.fetch_fear_greed_index()
    data_fetcher.fetch_google_trends()
    data_fetcher.fetch_macro_indicators()
    data_fetcher.fetch_bitcoin_circulating_supply()
    data_fetcher.fetch_bitcoin_dominance() 
    st.sidebar.text("API data fetch complete.")

    # Step 2: Calculate derived metrics
    st.sidebar.text("Calculating derived metrics...")
    formulas.calculate_pi_cycle_top()
    formulas.calculate_200wma()
    formulas.calculate_s2f_model()
    formulas.calculate_puell_multiple_alternative()
    st.sidebar.text("Derived metrics calculation complete.")
    
    # Store last successful update time
    update_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("data/last_successful_update.txt", "w") as f:
            f.write(update_time_str)
        st.sidebar.info(f"[{update_time_str}] Data update cycle finished.")
    except Exception as e:
        st.sidebar.warning(f"Could not write last update time: {e}")


# --- SIDEBAR ---
st.sidebar.header("About")
st.sidebar.info(
    "This dashboard monitors key indicators that have historically signaled Bitcoin bull cycle tops. "
    "It provides insights based on free data sources and calculated metrics. "
    "Thresholds are based on historical analysis and should be used as part of a broader investment strategy. "
    "This is not financial advice."
)
st.sidebar.header("Data Operations")
if st.sidebar.button("Fetch Latest Data & Refresh Dashboard", key="fetch_data_button"):
    with st.spinner("Refreshing all data... This may take a few minutes. The app will pause."):
        try:
            trigger_data_update_and_calculations()
            st.sidebar.success("Data refresh cycle complete! Rerunning dashboard...")
            time.sleep(0.5) 
            st.rerun() 
        except Exception as e:
            st.sidebar.error(f"Error during data refresh: {e}")
            st.sidebar.text_area("Error Details:", value=traceback.format_exc(), height=200)

try:
    with open("data/last_successful_update.txt", "r") as f:
        last_update_time_str = f.read().strip()
    st.sidebar.caption(f"Data last successfully fetched via button/manual run: {last_update_time_str}")
except FileNotFoundError:
    st.sidebar.caption("Data fetch status: N/A (run 'Fetch Latest Data' or ensure scheduler is active)")


# --- Initialize overall_risk_signals for each run ---
overall_risk_signals = {'Red': 0, 'Yellow': 0, 'Green': 0, 'NA': 0}

# --- DEFINE COLUMNS ONCE for the main layout ---
col1, col2, col3 = st.columns(3)

# --- METRIC 1: Bitcoin Price & ETH Price ---
with col1:
    st.subheader("📉 Market Prices (USD)")
    btc_price_df = fetch_from_db_dash("SELECT * FROM crypto_prices WHERE coin_id = 'bitcoin' ORDER BY timestamp DESC LIMIT 365")
    eth_price_df = fetch_from_db_dash("SELECT * FROM crypto_prices WHERE coin_id = 'ethereum' ORDER BY timestamp DESC LIMIT 365")

    if not btc_price_df.empty:
        latest_btc_price = btc_price_df.iloc[0]['price']
        st.metric(label="Bitcoin Price", value=f"${latest_btc_price:,.2f}" if latest_btc_price is not None else "N/A")
        if 'date' in btc_price_df.columns and 'price' in btc_price_df.columns:
            fig_btc = px.line(btc_price_df.sort_values(by='date'), x='date', y='price', title='BTC Price (Last Year)')
            st.plotly_chart(fig_btc, use_container_width=True)
    else:
        st.write("Bitcoin price data not available.")
        overall_risk_signals['NA'] +=1

    if not eth_price_df.empty:
        latest_eth_price = eth_price_df.iloc[0]['price']
        st.metric(label="Ethereum Price", value=f"${latest_eth_price:,.2f}" if latest_eth_price is not None else "N/A")
    else:
        st.write("Ethereum price data not available.")

# --- METRIC 2: Fear & Greed Index ---
with col2:
    st.subheader("😟 Fear & Greed Index")
    fg_df = fetch_from_db_dash("SELECT * FROM fear_greed_index ORDER BY timestamp DESC LIMIT 365")
    if not fg_df.empty:
        latest_fg = fg_df.iloc[0]
        fg_value = int(latest_fg['value']) if latest_fg['value'] is not None and pd.notna(latest_fg['value']) else None
        fg_classification_api = latest_fg['value_classification']
        
        color = "grey"; risk_description = "Data N/A"
        if fg_value is not None:
            risk_description = fg_classification_api if fg_classification_api else "Neutral" 
            color = "green"; overall_risk_signals['Green'] += 1 
            if fg_value >= th.FG_EXTREME_GREED: color = "red"; overall_risk_signals['Red'] += 1; risk_description = "Extreme Greed"
            elif fg_value >= th.FG_GREED: color = "orange"; overall_risk_signals['Yellow'] += 1; risk_description = "Greed"
        else: overall_risk_signals['NA'] +=1
            
        st.metric(label="Current F&G", value=f"{fg_value if fg_value is not None else 'N/A'} ({fg_classification_api if fg_classification_api else 'N/A'})")
        st.markdown(f"**Risk Level:** <span style='color:{color}; font-weight:bold;'>{risk_description}</span>", unsafe_allow_html=True)
        
        if 'date' in fg_df.columns and 'value' in fg_df.columns:
            fig_fg = px.line(fg_df.sort_values(by='date'), x='date', y='value', title='Fear & Greed Index (Last Year)')
            fig_fg.add_hline(y=th.FG_EXTREME_GREED, line_dash="dash", line_color="red", annotation_text="Extreme Greed Zone")
            fig_fg.add_hline(y=th.FG_GREED, line_dash="dash", line_color="orange", annotation_text="Greed Zone")
            st.plotly_chart(fig_fg, use_container_width=True)
    else:
        st.write("Fear & Greed data not available."); overall_risk_signals['NA'] +=1

# --- METRIC 3: Google Trends ---
with col3:
    st.subheader("🔍 Google Trends ('Bitcoin')")
    gt_df = fetch_from_db_dash("SELECT * FROM google_trends ORDER BY date DESC LIMIT 365")
    if not gt_df.empty:
        latest_gt_val = gt_df.iloc[0]['bitcoin_trends'] if 'bitcoin_trends' in gt_df.columns and not gt_df.empty and pd.notna(gt_df.iloc[0]['bitcoin_trends']) else None
        st.metric(label="Latest Trend Score", value=f"{latest_gt_val if latest_gt_val is not None else 'N/A'}")
        
        color = "grey"; risk_description="Data N/A"
        if latest_gt_val is not None:
            risk_description="Low"; color = "green"; overall_risk_signals['Green'] += 1
            if latest_gt_val >= th.GTRENDS_HIGH_RISK: color = "red"; overall_risk_signals['Red'] += 1; risk_description="High"
            elif latest_gt_val >= th.GTRENDS_MEDIUM_RISK: color = "orange"; overall_risk_signals['Yellow'] += 1; risk_description="Medium"
        else: overall_risk_signals['NA'] +=1
        
        st.markdown(f"**Retail FOMO Risk:** <span style='color:{color}; font-weight:bold;'>{risk_description}</span>", unsafe_allow_html=True)
        if 'date' in gt_df.columns and 'bitcoin_trends' in gt_df.columns:
            fig_gt = px.line(gt_df.sort_values(by='date'), x='date', y='bitcoin_trends', title="Google Trends for 'Bitcoin' (Last Year)")
            st.plotly_chart(fig_gt, use_container_width=True)
    else:
        st.write("Google Trends data not available."); overall_risk_signals['NA'] +=1

st.divider()
col4, col5, col6 = st.columns(3)

# --- METRIC 4: Pi Cycle Top ---
with col4:
    st.subheader("🥧 Pi Cycle Top")
    pi_df = fetch_from_db_dash("SELECT * FROM pi_cycle_data ORDER BY timestamp DESC LIMIT 1") 
    
    if not pi_df.empty:
        latest_pi = pi_df.iloc[0]
        current_sma_111 = latest_pi['sma_111'] if 'sma_111' in latest_pi and pd.notna(latest_pi['sma_111']) else None
        current_sma_350_doubled = latest_pi['sma_350_doubled'] if 'sma_350_doubled' in latest_pi and pd.notna(latest_pi['sma_350_doubled']) else None
        
        color = "grey"; risk_description = "Data N/A"
        if current_sma_111 is not None and current_sma_350_doubled is not None:
            risk_description = "Low"; color = "green"; overall_risk_signals['Green'] +=1
            if current_sma_111 >= current_sma_350_doubled:
                color = "red"; overall_risk_signals['Red'] +=1; risk_description = "High Risk (CROSSED)"
            elif current_sma_111 >= (th.PI_CYCLE_APPROACH_FACTOR * current_sma_350_doubled):
                color = "orange"; overall_risk_signals['Yellow'] +=1; risk_description = "Medium Risk (Approaching)"
        else: overall_risk_signals['NA'] +=1
            
        st.markdown(f"**Signal:** <span style='color:{color}; font-weight:bold;'>{risk_description}</span>", unsafe_allow_html=True)
        
        sma_111_display = f"{current_sma_111:.0f}" if current_sma_111 is not None else "N/A"
        sma_350_doubled_display = f"{current_sma_350_doubled:.0f}" if current_sma_350_doubled is not None else "N/A"
        st.caption(f"111DMA: {sma_111_display} | 350DMA*2: {sma_350_doubled_display}")

        pi_chart_df = fetch_from_db_dash("SELECT * FROM pi_cycle_data ORDER BY timestamp DESC LIMIT 730")
        if not pi_chart_df.empty and 'date' in pi_chart_df and 'btc_price' in pi_chart_df and 'sma_111' in pi_chart_df and 'sma_350_doubled' in pi_chart_df :
            fig_pi = go.Figure()
            fig_pi.add_trace(go.Scatter(x=pi_chart_df['date'].sort_values(), y=pi_chart_df['btc_price'], mode='lines', name='BTC Price'))
            fig_pi.add_trace(go.Scatter(x=pi_chart_df['date'].sort_values(), y=pi_chart_df['sma_111'], mode='lines', name='111DMA'))
            fig_pi.add_trace(go.Scatter(x=pi_chart_df['date'].sort_values(), y=pi_chart_df['sma_350_doubled'], mode='lines', name='350DMA x 2'))
            fig_pi.update_layout(title='Pi Cycle Top Indicator (Last ~2 Years)', legend_title_text='Metrics')
            st.plotly_chart(fig_pi, use_container_width=True)
        else:
            st.write("Chart data for Pi Cycle not available.") # This message might appear if table is empty.
    else:
        st.write("Pi Cycle Top data not available."); overall_risk_signals['NA'] +=1

# --- METRIC 5: 200 Week MA ---
with col5:
    st.subheader("🌊 Bitcoin Price vs. 200 Week MA")
    wma_df = fetch_from_db_dash("SELECT * FROM wma_200_data ORDER BY timestamp DESC LIMIT 1") 
    
    if not wma_df.empty:
        latest_wma = wma_df.iloc[0]
        btc_price_for_wma = latest_wma['btc_price'] if 'btc_price' in latest_wma and pd.notna(latest_wma['btc_price']) else None
        wma200_value = latest_wma['wma_200'] if 'wma_200' in latest_wma and pd.notna(latest_wma['wma_200']) else None
        
        price_to_wma_ratio = None
        if wma200_value and wma200_value > 0 and btc_price_for_wma is not None: 
            price_to_wma_ratio = btc_price_for_wma / wma200_value
        
        risk_html = get_risk_color_html(price_to_wma_ratio, th.WMA200_PRICE_RATIO_HIGH, th.WMA200_PRICE_RATIO_MEDIUM)
        st.markdown(f"**BTC Price / 200WMA Ratio:** {risk_html}", unsafe_allow_html=True)
        
        if price_to_wma_ratio is not None:
            if price_to_wma_ratio >= th.WMA200_PRICE_RATIO_HIGH: overall_risk_signals['Red'] +=1
            elif price_to_wma_ratio >= th.WMA200_PRICE_RATIO_MEDIUM: overall_risk_signals['Yellow'] +=1
            else: overall_risk_signals['Green'] +=1
        else:
            overall_risk_signals['NA'] +=1 
        
        btc_price_display = f"${btc_price_for_wma:,.0f}" if btc_price_for_wma is not None else "N/A"
        wma200_display = f"${wma200_value:,.0f}" if wma200_value is not None else "N/A (Insufficient History)"
        st.caption(f"Latest Weekly Price: {btc_price_display} | 200WMA: {wma200_display}")
        
        wma_chart_df = fetch_from_db_dash("SELECT * FROM wma_200_data ORDER BY timestamp") 
        if not wma_chart_df.empty and 'date' in wma_chart_df and 'btc_price' in wma_chart_df and 'wma_200' in wma_chart_df:
            fig_wma = go.Figure()
            fig_wma.add_trace(go.Scatter(x=wma_chart_df['date'].sort_values(), y=wma_chart_df['btc_price'], mode='lines', name='BTC Price (Weekly Close)'))
            fig_wma.add_trace(go.Scatter(x=wma_chart_df['date'].sort_values(), y=wma_chart_df['wma_200'], mode='lines', name='200 Week MA'))
            fig_wma.update_layout(title='Bitcoin Price vs 200 Week MA', yaxis_type="log")
            st.plotly_chart(fig_wma, use_container_width=True)
        else:
             st.write("Chart data for 200WMA not available (likely insufficient history).")
    else:
        st.write("200WMA data not available (likely due to insufficient historical price data)."); overall_risk_signals['NA'] +=1

# --- METRIC 6: Bitcoin Dominance ---
with col6:
    st.subheader("👑 Bitcoin Dominance")
    dom_df = fetch_from_db_dash("SELECT * FROM bitcoin_dominance ORDER BY timestamp DESC LIMIT 1") 
    if not dom_df.empty:
        latest_dominance = dom_df.iloc[0]['dominance'] if 'dominance' in dom_df.columns and pd.notna(dom_df.iloc[0]['dominance']) else None
        risk_html = get_risk_color_html(latest_dominance, th.DOMINANCE_FROTH_HIGH, th.DOMINANCE_FROTH_MEDIUM, low_is_good=False, value_format=".2f") 
        st.metric(label="Current BTC.D", value=f"{latest_dominance:.2f}%" if latest_dominance is not None else "N/A")
        st.markdown(f"**Market Froth Risk (Low BTC.D):** {risk_html}", unsafe_allow_html=True)

        if latest_dominance is not None:
            if latest_dominance <= th.DOMINANCE_FROTH_HIGH: overall_risk_signals['Red'] +=1
            elif latest_dominance <= th.DOMINANCE_FROTH_MEDIUM: overall_risk_signals['Yellow'] +=1
            else: overall_risk_signals['Green'] +=1
        else:
            overall_risk_signals['NA'] +=1
        
        dom_chart_df = fetch_from_db_dash("SELECT * FROM bitcoin_dominance ORDER BY timestamp DESC LIMIT 365")
        if len(dom_chart_df) > 1 and 'date' in dom_chart_df and 'dominance' in dom_chart_df: 
            fig_dom = px.line(dom_chart_df.sort_values(by='date'), x='date', y='dominance', title='Bitcoin Dominance (Daily Snapshots - Last Year)')
            st.plotly_chart(fig_dom, use_container_width=True)
        else:
            st.caption("Displaying current dominance snapshot. Historical chart populates over time.")
    else:
        st.write("Bitcoin Dominance data not available."); overall_risk_signals['NA'] +=1

st.divider()
col7, col8, col9 = st.columns(3) 

# --- METRIC 7: Stock-to-Flow Model ---
with col7:
    st.subheader("⛏️ Stock-to-Flow Model (BTC)")
    s2f_df = fetch_from_db_dash("SELECT * FROM s2f_data ORDER BY timestamp DESC LIMIT 1") 
    if not s2f_df.empty:
        latest_s2f = s2f_df.iloc[0]
        btc_price_s2f = latest_s2f['btc_price'] if 'btc_price' in latest_s2f and pd.notna(latest_s2f['btc_price']) else None
        s2f_model_price = latest_s2f['s2f_price_model'] if 's2f_price_model' in latest_s2f and pd.notna(latest_s2f['s2f_price_model']) else None
        s2f_ratio_val = latest_s2f['s2f_ratio'] if 's2f_ratio' in latest_s2f and pd.notna(latest_s2f['s2f_ratio']) else None
        
        deviation = None
        if s2f_model_price and s2f_model_price > 0 and btc_price_s2f is not None:
            deviation = btc_price_s2f / s2f_model_price
        
        risk_html = get_risk_color_html(deviation, th.S2F_PRICE_DEVIATION_HIGH, th.S2F_PRICE_DEVIATION_MEDIUM)
        st.markdown(f"**Price / S2F Model Ratio:** {risk_html}", unsafe_allow_html=True)

        if deviation is not None:
            if deviation >= th.S2F_PRICE_DEVIATION_HIGH: overall_risk_signals['Red'] +=1
            elif deviation >= th.S2F_PRICE_DEVIATION_MEDIUM: overall_risk_signals['Yellow'] +=1
            else: overall_risk_signals['Green'] +=1
        else: overall_risk_signals['NA'] +=1
        
        s2f_ratio_display = f"{s2f_ratio_val:.2f}" if s2f_ratio_val is not None else "N/A"
        s2f_model_price_display = f"${s2f_model_price:,.0f}" if s2f_model_price is not None else "N/A"
        st.caption(f"S2F Ratio: {s2f_ratio_display} | Model Price: {s2f_model_price_display}")

        s2f_chart_df = fetch_from_db_dash("SELECT * FROM s2f_data ORDER BY timestamp DESC LIMIT 365*4") 
        if not s2f_chart_df.empty and 'date' in s2f_chart_df and 'btc_price' in s2f_chart_df and 's2f_price_model' in s2f_chart_df:
            fig_s2f = go.Figure()
            fig_s2f.add_trace(go.Scatter(x=s2f_chart_df['date'].sort_values(), y=s2f_chart_df['btc_price'], mode='lines', name='BTC Price'))
            fig_s2f.add_trace(go.Scatter(x=s2f_chart_df['date'].sort_values(), y=s2f_chart_df['s2f_price_model'], mode='lines', name='S2F Model Price', line=dict(dash='dash')))
            fig_s2f.update_layout(title='Bitcoin Price vs. Stock-to-Flow Model', yaxis_type="log")
            st.plotly_chart(fig_s2f, use_container_width=True)
    else:
        st.write("Stock-to-Flow data not available."); overall_risk_signals['NA'] +=1

# --- METRIC 8: Puell Multiple (Calculated) ---
with col8:
    st.subheader("🏭 Puell Multiple (Calculated)")
    puell_df = fetch_from_db_dash("SELECT * FROM puell_multiple_calculated ORDER BY timestamp DESC LIMIT 1")
    if not puell_df.empty:
        latest_puell_val = puell_df.iloc[0]['puell_multiple'] if 'puell_multiple' in puell_df.columns and pd.notna(puell_df.iloc[0]['puell_multiple']) else None
        risk_html = get_risk_color_html(latest_puell_val, th.PUELL_HIGH_RISK, th.PUELL_MEDIUM_RISK)
        st.markdown(f"**Current Puell Multiple:** {risk_html}", unsafe_allow_html=True)

        if latest_puell_val is not None:
            if latest_puell_val >= th.PUELL_HIGH_RISK: overall_risk_signals['Red'] +=1
            elif latest_puell_val >= th.PUELL_MEDIUM_RISK: overall_risk_signals['Yellow'] +=1
            else: overall_risk_signals['Green'] +=1
        else: overall_risk_signals['NA'] +=1
        
        puell_chart_df = fetch_from_db_dash("SELECT * FROM puell_multiple_calculated ORDER BY timestamp DESC LIMIT 365*2")
        if not puell_chart_df.empty and 'date' in puell_chart_df and 'puell_multiple' in puell_chart_df:
            fig_puell = px.line(puell_chart_df.sort_values(by='date'), x='date', y='puell_multiple', title='Puell Multiple (Calculated)')
            fig_puell.add_hline(y=th.PUELL_HIGH_RISK, line_dash="dash", line_color="red", annotation_text="High Risk Zone")
            fig_puell.add_hline(y=th.PUELL_MEDIUM_RISK, line_dash="dash", line_color="orange", annotation_text="Medium Risk Zone")
            st.plotly_chart(fig_puell, use_container_width=True)
        else:
             st.write("Chart data for Puell Multiple not available (may need a few more days of price data).")
    else:
        st.write("Puell Multiple data not available (may need a few more days of price data)."); overall_risk_signals['NA'] +=1

# --- METRIC 9: Macro Indicators ---
with col9: 
    st.subheader("🌍 Macro Indicators (Latest)")
    macro_tickers_to_display = ['SPX', 'Gold', 'DXY', 'US10Y']
    for ticker_name in macro_tickers_to_display:
        df_macro = fetch_from_db_dash(f"SELECT * FROM macro_indicators WHERE ticker = '{ticker_name}' ORDER BY date DESC LIMIT 1") 
        if not df_macro.empty:
            latest_val = df_macro.iloc[0]['close_price'] if 'close_price' in df_macro.columns and pd.notna(df_macro.iloc[0]['close_price']) else None
            st.metric(label=f"{ticker_name} Latest Close", value=f"{latest_val:,.2f}" if latest_val is not None else "N/A")
        else:
            st.write(f"{ticker_name} data not available."); overall_risk_signals['NA'] +=1
    st.caption("Individual charts for macro indicators can be added by fetching more history for display.")


st.divider()
# --- OVERALL RISK ASSESSMENT ---
st.header("🚦 Overall Market Risk Assessment")
countable_indicators = overall_risk_signals['Green'] + overall_risk_signals['Yellow'] + overall_risk_signals['Red']
if countable_indicators > 0:
    st.markdown(
        f"**Active Signals Breakdown:** <span style='color:red;'>{overall_risk_signals['Red']} High</span> | "
        f"<span style='color:orange;'>{overall_risk_signals['Yellow']} Medium</span> | "
        f"<span style='color:green;'>{overall_risk_signals['Green']} Low</span> "
        f"(out of {countable_indicators} indicators with conclusive signals). "
        f"{overall_risk_signals['NA']} indicators have N/A data.",
        unsafe_allow_html=True)

    overall_color = "green"; overall_text = "Low Overall Market Risk"; details = [] 
    if overall_risk_signals['Red'] >= th.OVERALL_HIGH_RISK_COUNT_RED:
        overall_color = "red"; overall_text = "High Overall Market Risk - Extreme Caution Advised!"
        details.append(f"Reason: {overall_risk_signals['Red']} indicators signaling High Risk (Threshold: >={th.OVERALL_HIGH_RISK_COUNT_RED}).")
    
    elif overall_color != "red": # This check is now implicitly handled by the elif structure
        condition_medium_by_red_count = (overall_risk_signals['Red'] >= th.OVERALL_MEDIUM_RISK_COUNT_RED)
        condition_medium_by_sum = ((overall_risk_signals['Red'] + overall_risk_signals['Yellow']) >= th.OVERALL_MEDIUM_RISK_SUM_YELLOW_RED)
        if condition_medium_by_red_count or condition_medium_by_sum:
            overall_color = "orange"; overall_text = "Elevated Overall Market Risk - Caution Advised."
            # Provide reasons in order of precedence or combine if both contribute
            if condition_medium_by_red_count:
                 details.append(f"Reason: {overall_risk_signals['Red']} High Risk signals (Medium Overall threshold: >={th.OVERALL_MEDIUM_RISK_COUNT_RED}).")
            if condition_medium_by_sum and not (condition_medium_by_red_count and th.OVERALL_MEDIUM_RISK_COUNT_RED == th.OVERALL_HIGH_RISK_COUNT_RED) : # Avoid redundant messaging if already red due to count
                details.append(f"Reason: Sum of {overall_risk_signals['Red'] + overall_risk_signals['Yellow']} High/Medium signals (Sum threshold: >={th.OVERALL_MEDIUM_RISK_SUM_YELLOW_RED}).")
        else: 
            details.append("Reason: Number of High/Medium risk indicators below defined thresholds for elevated risk.")

    st.markdown(f"### <span style='color:{overall_color};'>{overall_text}</span>", unsafe_allow_html=True)
    if details:
        for detail_item in details: st.caption(detail_item)
    
    current_risk_points = (overall_risk_signals['Red'] * 2) + (overall_risk_signals['Yellow'] * 1)
    max_possible_points_from_active = countable_indicators * 2 
    if max_possible_points_from_active > 0:
        risk_percentage = (current_risk_points / max_possible_points_from_active)
        st.progress(risk_percentage) 
else:
    st.write("Not enough conclusive signals from indicators to assess overall market risk.")