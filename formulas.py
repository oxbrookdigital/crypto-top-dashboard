# formulas.py
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, timezone
import numpy as np 

DB_PATH = 'data/crypto_metrics.db'

def get_btc_price_data_from_db(days_history=None, end_date_dt=None):
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT timestamp, price FROM crypto_prices WHERE coin_id = 'bitcoin' ORDER BY timestamp"
    
    if days_history:
        if end_date_dt is None:
            end_date_dt = datetime.now(timezone.utc)
        start_timestamp = int((end_date_dt - timedelta(days=days_history)).timestamp())
        query = f"SELECT timestamp, price FROM crypto_prices WHERE coin_id = 'bitcoin' AND timestamp >= {start_timestamp} ORDER BY timestamp"

    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Error reading BTC price data from DB: {e}")
        df = pd.DataFrame(columns=['timestamp', 'price']) # Return empty df on error
    finally:
        conn.close()

    if not df.empty:
        df['date'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df.set_index('date', inplace=True)
        df = df['price'].rename('close') # Assuming 'price' is the close price
        df = df[~df.index.duplicated(keep='last')] # Ensure unique index
    return df

def calculate_pi_cycle_top():
    print("Calculating Pi Cycle Top...")
    # Need at least 350 days of data for the longer MA. Fetch a bit more for buffer.
    btc_df = get_btc_price_data_from_db(days_history=400) 
    if btc_df.empty or len(btc_df) < 350:
        print(f"Not enough Bitcoin price data (need 350, have {len(btc_df)}) to calculate Pi Cycle Top.")
        return

    pi_df = pd.DataFrame(index=btc_df.index)
    pi_df['btc_price'] = btc_df

    pi_df['sma_111'] = btc_df.rolling(window=111, min_periods=111).mean()
    pi_df['sma_350_doubled'] = btc_df.rolling(window=350, min_periods=350).mean() * 2
    
    pi_df['signal'] = 'Neutral'
    if 'sma_111' in pi_df.columns and 'sma_350_doubled' in pi_df.columns: # Ensure MAs were calculated
        # Condition for "High Risk (Crossed)"
        condition_crossed = pi_df['sma_111'] >= pi_df['sma_350_doubled']
        pi_df.loc[condition_crossed, 'signal'] = 'High Risk (111DMA >= 350DMA*2 - CROSSED)'

        # Condition for "Approaching" (only if not already crossed)
        # This condition makes more sense if sma_111 has not yet crossed but is close.
        # For simplicity, the dashboard can handle the "approaching" visual if sma_111 is < sma_350_doubled but close.
        # The 'signal' column will mainly reflect the crossed state.
    
    pi_df.dropna(subset=['sma_111', 'sma_350_doubled'], inplace=True) # Only keep rows where MAs are valid
    if pi_df.empty:
        print("Pi Cycle DataFrame is empty after dropping NA from MAs.")
        return

    pi_df.reset_index(inplace=True)
    pi_df['timestamp'] = pi_df['date'].apply(lambda x: int(x.timestamp()))
    pi_df = pi_df[['timestamp', 'btc_price', 'sma_111', 'sma_350_doubled', 'signal']]
    
    conn = sqlite3.connect(DB_PATH)
    pi_df.to_sql('pi_cycle_data', conn, if_exists='replace', index=False) # Replace as it's recalculated fully
    conn.close()
    print("Pi Cycle Top data calculated and stored.")


def calculate_200wma():
    print("Calculating 200 Week Moving Average...")
    # Need at least 200 weeks * 7 days/week = 1400 days. Fetch a bit more.
    btc_daily_df = get_btc_price_data_from_db(days_history=1500) 
    if btc_daily_df.empty or len(btc_daily_df) < 1400: # Need enough daily points for weekly resampling
        print(f"Not enough Bitcoin daily price data (need ~1400, have {len(btc_daily_df)}) for 200WMA.")
        return

    btc_weekly_df = btc_daily_df.resample('W-SUN').last() # Week ending on Sunday
    if btc_weekly_df.empty or len(btc_weekly_df) < 200:
        print(f"Not enough weekly data points (need 200, have {len(btc_weekly_df)}) after resampling for 200WMA.")
        return

    wma_df = pd.DataFrame(index=btc_weekly_df.index)
    wma_df['btc_price'] = btc_weekly_df # This is weekly closing price
    wma_df['wma_200'] = btc_weekly_df.rolling(window=200, min_periods=200).mean()
    
    wma_df.dropna(subset=['wma_200'], inplace=True) # Only keep rows where 200WMA is valid
    if wma_df.empty:
        print("200WMA DataFrame is empty after dropping NA from WMA calculation.")
        return
        
    wma_df.reset_index(inplace=True)
    wma_df['timestamp'] = wma_df['date'].apply(lambda x: int(x.timestamp()))
    # Ensure correct columns are selected for storage
    wma_df = wma_df[['timestamp', 'btc_price', 'wma_200']]


    conn = sqlite3.connect(DB_PATH)
    wma_df.to_sql('wma_200_data', conn, if_exists='replace', index=False) # Replace, recalculated fully
    conn.close()
    print("200WMA data calculated and stored.")

def calculate_s2f_model():
    print("Calculating Stock-to-Flow Model...")
    # Fetch a good range of price history for context if plotting S2F against price
    btc_df = get_btc_price_data_from_db(days_history=365*5) # 5 years of price data
    if btc_df.empty:
        print("No Bitcoin price data for S2F calculations.")
        return

    current_circulating_supply = None
    conn_supply = sqlite3.connect(DB_PATH)
    try:
        supply_df = pd.read_sql_query("SELECT circulating_supply FROM bitcoin_supply_info ORDER BY timestamp DESC LIMIT 1", conn_supply)
        if not supply_df.empty and supply_df.iloc[0]['circulating_supply'] is not None:
            current_circulating_supply = supply_df.iloc[0]['circulating_supply']
        else:
            print("Circulating supply not found in database for S2F. Please run data_fetcher.")
            return
    except Exception as e:
        print(f"Error reading circulating supply from DB for S2F: {e}")
        return
    finally:
        conn_supply.close()
    
    if current_circulating_supply is None: return


    # S2F Parameters - these need to be accurate and potentially dynamic for full historical accuracy
    # For this version, we use current block reward and assume it for S2F ratio calculation
    # A truly historical S2F model would adjust block reward based on past halving dates.
    # Halving dates (approximate, for future refinement if needed):
    # H1: 2012-11-28 (50 -> 25)
    # H2: 2016-07-09 (25 -> 12.5)
    # H3: 2020-05-11 (12.5 -> 6.25)
    # H4: 2024-04-19 (6.25 -> 3.125)
    current_block_reward_btc = 3.125 
    blocks_per_day_approx = (24 * 60) / 10 # Assuming ~10 min block time = 144
    annual_flow_btc = current_block_reward_btc * blocks_per_day_approx * 365.25

    s2f_ratio = current_circulating_supply / annual_flow_btc if annual_flow_btc > 0 else 0
    
    # PlanB's S2F Model Price (one common formulation): Price = a * (S2F_Ratio ^ n)
    # Parameters 'a' and 'n' are derived from regression on historical data.
    # Using commonly cited parameters: a = exp(-1.84) or 0.1586, n = 3.36 (approximately)
    # These can vary between different analyses of the S2F model.
    # For consistency, let's use one set: Price = 0.204 * (S2F_Ratio ^ 3.268) - from a popular chart.
    # Or an often quoted one: Price = exp(-1.84) * (S2F_Ratio ** 3.36)
    # Let's use the one from earlier: model_param_a = 0.0216; model_param_n = 3.306; (these might be for a specific scale)
    # A widely known formula from PlanB's 2019 article is approximately: Price = 0.4 * S2F^3 (very simplified)
    # Or more precisely from his paper: ln(market value) = 3.3 * ln(SF) + 14.6 => market value = exp(14.6) * SF^3.3
    # Price = (exp(14.6) * SF^3.3) / circulating_supply.
    # For dashboard simplicity, many charts show a direct S2F_Model_Price = Parameter * (S2F_Ratio ^ Exponent)
    
    # Using a common formulation often seen in charts:
    s2f_model_price_value = np.exp(14.6) * (s2f_ratio**3.3) / current_circulating_supply if current_circulating_supply > 0 else 0
    # This formula above results in a "market value" then divides by supply, which is just the SF^n * param part.
    # Let's use a direct price model version: Price = Parameter_A * (S2F_Ratio ^ Parameter_N)
    # Common parameters found in some analyses: Parameter_A ~ 25000 to 55000 (when S2F ratio is around 25-50)
    # This part is highly model-specific. For now, let's use a simplified version for demonstration.
    # A simplified model (ensure you research/validate your chosen S2F formula and parameters):
    # Based on exp(14.6) * (SF^3.3) as market cap, then price is this divided by stock.
    # Price = (exp(14.6) * (Stock/Flow)^3.3) / Stock = exp(14.6) * Stock^2.3 * Flow^-3.3
    # This indicates the "Model Price" often refers to a price level, not just the ratio.
    # Let's use a simplified direct price model from a known source if possible, or PlanB's formula.
    # Plan B's 2019 formula: market_value_USD = exp(14.607) * S2F_Ratio ^ 3.3168
    # So, S2F_Model_Price = (np.exp(14.607) * (s2f_ratio ** 3.3168)) / current_circulating_supply
    # This seems more correct for a per-BTC model price.
    if s2f_ratio > 0 and current_circulating_supply > 0:
         s2f_model_price_value = (np.exp(14.607) * (s2f_ratio ** 3.3168)) / current_circulating_supply
    else:
        s2f_model_price_value = 0

    s2f_data_list = []
    for date_index, btc_close_price in btc_df.items():
        s2f_data_list.append({
            'timestamp': int(date_index.timestamp()),
            'btc_price': btc_close_price,
            's2f_ratio': s2f_ratio, # Using current S2F ratio across the historical price chart for simplicity
            's2f_price_model': s2f_model_price_value # Using current model price across history
        })
        
    if s2f_data_list:
        s2f_df_to_store = pd.DataFrame(s2f_data_list)
        conn_s2f = sqlite3.connect(DB_PATH) 
        s2f_df_to_store.to_sql('s2f_data', conn_s2f, if_exists='replace', index=False) # Replace, recalculated fully
        conn_s2f.close()
        print(f"S2F Model data calculated (current S2F ratio: {s2f_ratio:.2f}, model price: ${s2f_model_price_value:,.2f}) and stored.")

def calculate_puell_multiple_alternative():
    print("Calculating Puell Multiple (Alternative)...")
    # Need at least 365 days for the MA. Fetch a bit more.
    btc_df = get_btc_price_data_from_db(days_history=400) 
    if btc_df.empty or len(btc_df) < 365:
        print(f"Not enough Bitcoin price data (need 365 days, have {len(btc_df)}) to calculate Puell Multiple accurately.")
        return

    # Current block reward (post-April 2024 halving) - for historical accuracy, this needs to change based on date.
    # For this simplified "alternative calculation", we'll use the current reward.
    current_block_reward_btc = 3.125  
    blocks_per_day_approx = 144 
    
    daily_issuance_btc = current_block_reward_btc * blocks_per_day_approx
    
    puell_df = pd.DataFrame(index=btc_df.index)
    puell_df['btc_price'] = btc_df
    puell_df['daily_issuance_usd'] = daily_issuance_btc * puell_df['btc_price']
    puell_df['daily_issuance_usd_365d_ma'] = puell_df['daily_issuance_usd'].rolling(window=365, min_periods=365).mean()
    puell_df['puell_multiple'] = puell_df['daily_issuance_usd'] / puell_df['daily_issuance_usd_365d_ma']

    puell_df.dropna(subset=['puell_multiple'], inplace=True) # Only keep rows where Puell is valid
    if puell_df.empty:
        print("Puell Multiple DataFrame is empty after dropping NA.")
        return

    puell_df.reset_index(inplace=True)
    puell_df['timestamp'] = puell_df['date'].apply(lambda x: int(x.timestamp()))
    puell_df = puell_df[['timestamp', 'btc_price', 'daily_issuance_usd', 'daily_issuance_usd_365d_ma', 'puell_multiple']]

    conn = sqlite3.connect(DB_PATH)
    puell_df.to_sql('puell_multiple_calculated', conn, if_exists='replace', index=False) # Replace, recalculated fully
    conn.close()
    print("Puell Multiple (Alternative) data calculated and stored.")


if __name__ == "__main__":
    print(f"[{datetime.now()}] Running formulas.py calculations (direct run)...")
    # Ensure DB is initialized (though data_fetcher should do this)
    # And price data is fetched before calculating
    conn_check = sqlite3.connect(DB_PATH)
    try:
        btc_prices_exist_check = pd.read_sql_query("SELECT COUNT(*) as count FROM crypto_prices WHERE coin_id = 'bitcoin'", conn_check).iloc[0,0] > 0
    except pd.io.sql.DatabaseError: # Table doesn't exist
        btc_prices_exist_check = False
    finally:
        conn_check.close()

    if not btc_prices_exist_check:
        print("Bitcoin price data not found in DB. Please run data_fetcher.py first.")
    else:
        print("Calculating derived metrics...")
        calculate_pi_cycle_top()
        calculate_200wma()
        calculate_s2f_model() 
        calculate_puell_multiple_alternative()
        print(f"[{datetime.now()}] Derived metrics calculation tasks (direct run) finished.")