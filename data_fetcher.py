# data_fetcher.py
import sqlite3
import pandas as pd
from pycoingecko import CoinGeckoAPI
from pytrends.request import TrendReq
import yfinance as yf
import requests
from datetime import datetime, timedelta, timezone
import time # For rate limiting

# --- CONFIGURATIONS ---
DB_PATH = 'data/crypto_metrics.db'
cg = CoinGeckoAPI() 

# --- DATABASE UTILITIES ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Bitcoin & Ethereum Prices
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS crypto_prices (
        timestamp INTEGER NOT NULL,
        coin_id TEXT NOT NULL,
        price REAL,
        market_cap REAL,
        total_volume REAL,
        PRIMARY KEY (timestamp, coin_id)
    )
    ''')

    # Fear & Greed Index
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fear_greed_index (
        timestamp INTEGER PRIMARY KEY,
        value INTEGER,
        value_classification TEXT
    )
    ''')

    # Google Trends
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS google_trends (
        date TEXT PRIMARY KEY,
        bitcoin_trends INTEGER
    )
    ''') 

    # Macro Indicators (SPX, Gold, DXY, US10Y)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS macro_indicators (
        date TEXT NOT NULL,
        ticker TEXT NOT NULL,
        close_price REAL,
        PRIMARY KEY (date, ticker)
    )
    ''') 

    # Bitcoin Dominance (stores daily snapshot of current dominance)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bitcoin_dominance (
        timestamp INTEGER PRIMARY KEY,
        dominance REAL
    )
    ''')
    
    # Calculated Pi Cycle Data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pi_cycle_data (
        timestamp INTEGER PRIMARY KEY,
        btc_price REAL,
        sma_111 REAL,
        sma_350_doubled REAL,
        signal TEXT 
    )
    ''')

    # Calculated 200WMA Data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS wma_200_data (
        timestamp INTEGER PRIMARY KEY, 
        btc_price REAL,
        wma_200 REAL
    )
    ''')
    
    # Stock-to-Flow Data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS s2f_data (
        timestamp INTEGER PRIMARY KEY,
        btc_price REAL,
        s2f_ratio REAL,
        s2f_price_model REAL
    )
    ''')

    # Calculated Puell Multiple Data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS puell_multiple_calculated (
        timestamp INTEGER PRIMARY KEY,
        btc_price REAL,
        daily_issuance_usd REAL,
        daily_issuance_usd_365d_ma REAL,
        puell_multiple REAL
    )
    ''')

    # Bitcoin Circulating Supply Info
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bitcoin_supply_info (
        timestamp INTEGER PRIMARY KEY,
        circulating_supply REAL
    )
    ''')

    conn.commit()
    conn.close()

def get_last_timestamp(table_name, coin_id=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = f"SELECT MAX(timestamp) FROM \"{table_name}\"" # Quote table name
    if coin_id:
        query += f" WHERE coin_id = '{coin_id}'"
    try:
        cursor.execute(query)
        result = cursor.fetchone()[0]
    except sqlite3.Error:
        result = None
    finally:
        conn.close()
    return result if result else 0 

def get_last_date_str(table_name, ticker_column=None, ticker_value=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = f"SELECT MAX(date) FROM \"{table_name}\"" # Quote table name
    if ticker_column and ticker_value:
        query += f" WHERE \"{ticker_column}\" = '{ticker_value}'"

    try:
        cursor.execute(query)
        result = cursor.fetchone()[0]
    except sqlite3.Error:
        result = None
    finally:
        conn.close()
    return result 

def store_data_incrementally(df_new_data, table_name, pk_columns_list):
    if df_new_data.empty:
        # print(f"No new data provided to store in {table_name}.") # Less verbose
        return

    df_new_data.columns = [str(col) for col in df_new_data.columns]
    pk_columns_list = [str(col) for col in pk_columns_list]

    conn = sqlite3.connect(DB_PATH)
    try:
        temp_composite_pk_col = '_temp_composite_pk_for_merge'
        
        if len(pk_columns_list) > 1:
            # Create composite keys for matching for new data
            df_new_data[temp_composite_pk_col] = df_new_data[pk_columns_list].astype(str).agg('_'.join, axis=1)
            
            # Query existing composite keys
            existing_pks_db_str_list = []
            for col in pk_columns_list:
                # Attempt to cast to TEXT, especially for date/timestamp columns which might be stored differently
                if 'date' in col.lower() or 'timestamp' in col.lower():
                    existing_pks_db_str_list.append(f'CAST("{col}" AS TEXT)')
                else:
                    existing_pks_db_str_list.append(f'"{col}"')
            existing_pks_db_str = " || '_' || ".join(existing_pks_db_str_list)
            existing_pks_query = f'SELECT DISTINCT {existing_pks_db_str} AS "{temp_composite_pk_col}" FROM "{table_name}"' # Quote table_name
            
        else: # Single primary key
            pk_col_name = pk_columns_list[0]
            existing_pks_query = f'SELECT DISTINCT "{pk_col_name}" FROM "{table_name}"' # Quote table_name
            
        try:
            df_existing_pks = pd.read_sql_query(existing_pks_query, conn)
        except pd.io.sql.DatabaseError: # Table might not exist on first run for this specific query
            df_existing_pks = pd.DataFrame()


        if not df_existing_pks.empty:
            if len(pk_columns_list) > 1:
                # We are comparing based on the temporary composite key
                df_to_insert = df_new_data[~df_new_data[temp_composite_pk_col].isin(df_existing_pks[temp_composite_pk_col])].copy()
            else: # Single PK
                pk_col_name = pk_columns_list[0]
                # Ensure type consistency for comparison with single PK
                if pd.api.types.is_datetime64_any_dtype(df_new_data[pk_col_name]) or 'date' in pk_col_name.lower():
                    df_new_data_pk_as_str = pd.to_datetime(df_new_data[pk_col_name]).dt.strftime('%Y-%m-%d')
                    existing_pks_as_str = pd.to_datetime(df_existing_pks[pk_col_name]).dt.strftime('%Y-%m-%d')
                    df_to_insert = df_new_data[~df_new_data_pk_as_str.isin(existing_pks_as_str)].copy()
                elif pd.api.types.is_numeric_dtype(df_new_data[pk_col_name]):
                    df_to_insert = df_new_data[~df_new_data[pk_col_name].astype(float).isin(df_existing_pks[pk_col_name].astype(float))].copy()
                else: # Assume string
                    df_to_insert = df_new_data[~df_new_data[pk_col_name].astype(str).isin(df_existing_pks[pk_col_name].astype(str))].copy()
        else: # No existing data, so all new data is to be inserted
            df_to_insert = df_new_data.copy()
        
        # Clean up temporary column if it was added
        if temp_composite_pk_col in df_to_insert.columns:
            df_to_insert.drop(columns=[temp_composite_pk_col], inplace=True)

        if not df_to_insert.empty:
            # Ensure only original columns are inserted
            original_columns = [col for col in df_new_data.columns if col != temp_composite_pk_col]
            df_to_insert[original_columns].to_sql(table_name, conn, if_exists='append', index=False)
            print(f"{len(df_to_insert)} new rows stored in {table_name}.")
        else:
            print(f"No new unique rows to store in {table_name} (all provided rows already exist or df was empty).")

    except Exception as e:
        print(f"Error in store_data_incrementally for {table_name}: {e}. New DF Dtypes:\n{df_new_data.dtypes if not df_new_data.empty else 'New DF Empty'}")
    finally:
        conn.close()

# --- DATA FETCHING FUNCTIONS ---
def fetch_crypto_prices(coin_ids=['bitcoin', 'ethereum'], vs_currency='usd', initial_days_fetch=360):
    print(f"Fetching prices for {coin_ids}...")
    today_timestamp = int(datetime.now(timezone.utc).timestamp())

    for coin_id in coin_ids:
        all_coin_data_for_this_id = [] 
        last_stored_timestamp = get_last_timestamp('crypto_prices', coin_id=coin_id)
        
        from_timestamp_str = ""
        to_timestamp_str = str(today_timestamp)

        if last_stored_timestamp > 0:
            from_timestamp_dt = datetime.fromtimestamp(last_stored_timestamp, timezone.utc) + timedelta(seconds=1)
            from_timestamp_str = str(int(from_timestamp_dt.timestamp()))
            # print(f"Last stored price for {coin_id} at {datetime.fromtimestamp(last_stored_timestamp, timezone.utc)}. Fetching new data from {from_timestamp_dt}...") # Less verbose
            
            if int(from_timestamp_str) >= today_timestamp:
                print(f"Price data for {coin_id} appears up to date.")
                continue 
        else:
            from_timestamp_dt = datetime.now(timezone.utc) - timedelta(days=initial_days_fetch)
            from_timestamp_str = str(int(from_timestamp_dt.timestamp()))
            print(f"No existing price data for {coin_id}. Fetching last {initial_days_fetch} days from ~{from_timestamp_dt.date()}...")

        try:
            chart_data = cg.get_coin_market_chart_range_by_id(
                id=coin_id, 
                vs_currency=vs_currency,
                from_timestamp=from_timestamp_str,
                to_timestamp=to_timestamp_str
            )
            time.sleep(3) # Increased sleep for CoinGecko

            if chart_data and 'prices' in chart_data and chart_data['prices']:
                prices_df = pd.DataFrame(chart_data['prices'], columns=['timestamp', 'price'])
                
                if 'market_caps' in chart_data and chart_data['market_caps']:
                     market_caps_df = pd.DataFrame(chart_data['market_caps'], columns=['timestamp', 'market_cap'])
                     prices_df = pd.merge(prices_df, market_caps_df, on='timestamp', how='left')
                else:
                    prices_df['market_cap'] = None
                
                if 'total_volumes' in chart_data and chart_data['total_volumes']:
                    total_volumes_df = pd.DataFrame(chart_data['total_volumes'], columns=['timestamp', 'total_volume'])
                    prices_df = pd.merge(prices_df, total_volumes_df, on='timestamp', how='left')
                else:
                    prices_df['total_volume'] = None
                
                prices_df['timestamp'] = prices_df['timestamp'] // 1000 
                prices_df['coin_id'] = coin_id
                prices_df = prices_df[prices_df['timestamp'] >= int(from_timestamp_str)] 
                all_coin_data_for_this_id.append(prices_df)
            # else: # Less verbose
                # print(f"No new price data found for {coin_id} from {datetime.fromtimestamp(int(from_timestamp_str), timezone.utc)}.")

        except Exception as e:
            print(f"Error fetching price for {coin_id} in range: {e}")

        if all_coin_data_for_this_id:
            final_df_coin = pd.concat(all_coin_data_for_this_id)
            if not final_df_coin.empty:
                store_data_incrementally(final_df_coin, 'crypto_prices', pk_columns_list=['timestamp', 'coin_id'])
    # print(f"Crypto prices update attempt finished for {coin_ids}.") # Less verbose


def fetch_fear_greed_index(initial_limit=0, daily_limit=30):
    print("Fetching Fear & Greed Index...")
    last_stored_timestamp = get_last_timestamp('fear_greed_index')
    
    current_fetch_limit = daily_limit
    if last_stored_timestamp == 0:
        current_fetch_limit = initial_limit
        print("No existing Fear & Greed data. Fetching all historical data...")
    # else: # Less verbose
        # print(f"Last Fear & Greed data at {datetime.fromtimestamp(last_stored_timestamp, timezone.utc)}. Fetching last {daily_limit} days for updates...")

    try:
        response = requests.get(f'https://api.alternative.me/fng/?limit={current_fetch_limit}&format=json')
        response.raise_for_status()
        data = response.json()['data']
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_numeric(df['timestamp'])
        df.rename(columns={'value_classification': 'value_classification'}, inplace=True) # Ensure column name consistency
        df = df[['timestamp', 'value', 'value_classification']] # Select and order columns
        
        store_data_incrementally(df, 'fear_greed_index', pk_columns_list=['timestamp'])
    except Exception as e:
        print(f"Error fetching Fear & Greed Index: {e}")

def fetch_google_trends(keyword="Bitcoin", initial_timeframe='today 5-y'):
    print(f"Fetching Google Trends for '{keyword}'...")
    pytrends = TrendReq(hl='en-US', tz=360)
    last_date_str_db = get_last_date_str('google_trends') 

    current_timeframe = initial_timeframe
    fetch_df = True
    if last_date_str_db:
        last_date_dt = datetime.strptime(last_date_str_db, '%Y-%m-%d').date()
        # Fetch a slightly wider window for recent daily trends, e.g., last 90 days, then filter
        # Pytrends daily data is more reliable over slightly longer recent periods than just a few days.
        start_date_fetch_for_recent = datetime.now(timezone.utc).date() - timedelta(days=90) # Fetch last 90 days
        if last_date_dt >= start_date_fetch_for_recent: # If our last data is within this recent window
             start_date_fetch_for_recent = last_date_dt + timedelta(days=1)

        end_date_fetch = datetime.now(timezone.utc).date()

        if start_date_fetch_for_recent <= end_date_fetch:
            current_timeframe = f"{start_date_fetch_for_recent.strftime('%Y-%m-%d')} {end_date_fetch.strftime('%Y-%m-%d')}"
            # print(f"Last Google Trends data on {last_date_str_db}. Fetching new data for timeframe: {current_timeframe}...") # Less verbose
        else:
            print("Google Trends data is up to date.")
            fetch_df = False
    else:
        print(f"No existing Google Trends data. Fetching for timeframe: {initial_timeframe}...")
    
    if fetch_df:
        try:
            pytrends.build_payload([keyword], cat=0, timeframe=current_timeframe, geo='', gprop='')
            df = pytrends.interest_over_time()
            time.sleep(1.5) 
            if not df.empty and keyword in df.columns:
                df_filtered = df[[keyword]].copy() # Use .copy()
                df_filtered.reset_index(inplace=True) 
                df_filtered.rename(columns={'date': 'date', keyword: 'bitcoin_trends'}, inplace=True)
                df_filtered['date'] = df_filtered['date'].dt.strftime('%Y-%m-%d')
                
                store_data_incrementally(df_filtered, 'google_trends', pk_columns_list=['date'])
            elif df.empty:
                 print(f"Google Trends returned empty dataframe for '{keyword}' for timeframe '{current_timeframe}'.")
            else: 
                print(f"Keyword '{keyword}' not found in Google Trends columns for timeframe '{current_timeframe}'.")
        except Exception as e:
            print(f"Error fetching Google Trends for timeframe '{current_timeframe}': {e}")


def fetch_macro_indicators(tickers={'^GSPC': 'SPX', 'GC=F': 'Gold', 'DX-Y.NYB': 'DXY', '^TNX': 'US10Y'}):
    print("Fetching Macro Indicators...")
    all_new_macro_data = []
    
    for ticker_symbol, name in tickers.items():
        last_date_str_db = get_last_date_str('macro_indicators', ticker_column='ticker', ticker_value=name)
        
        start_date_fetch_str = None
        fetch_this_ticker = True

        if last_date_str_db:
            start_date_fetch = datetime.strptime(last_date_str_db, '%Y-%m-%d').date() + timedelta(days=1)
            if start_date_fetch > datetime.now(timezone.utc).date():
                # print(f"Macro data for {name} is up to date (last: {last_date_str_db}).") # Less verbose
                fetch_this_ticker = False
            else:
                start_date_fetch_str = start_date_fetch.strftime('%Y-%m-%d')
                # print(f"Last macro data for {name} on {last_date_str_db}. Fetching new data from {start_date_fetch_str}...") # Less verbose
        else:
            print(f"No existing macro data for {name}. Fetching max history...")
        
        if fetch_this_ticker:
            try:
                if start_date_fetch_str: # Incremental fetch
                    data = yf.download(ticker_symbol, start=start_date_fetch_str, interval="1d", progress=False, auto_adjust=True)
                else: # Initial full history fetch
                    data = yf.download(ticker_symbol, period="max", interval="1d", progress=False, auto_adjust=True)
                time.sleep(1.5) 

                if not data.empty:
                    df = data[['Close']].copy() 
                    df.reset_index(inplace=True)
                    
                    # yfinance 'Date' column might be named 'index' or 'Date' or 'Datetime' after reset_index depending on version/data
                    date_col_name = 'Date' # Default
                    if 'index' in df.columns and pd.api.types.is_datetime64_any_dtype(df['index']):
                        date_col_name = 'index'
                    elif 'Datetime' in df.columns and pd.api.types.is_datetime64_any_dtype(df['Datetime']):
                        date_col_name = 'Datetime'
                    elif 'date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['date']): # if already lowercase
                        date_col_name = 'date'

                    df.rename(columns={date_col_name: 'date', 'Close': 'close_price'}, inplace=True)
                    df['ticker'] = name
                    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                    
                    df = df[['date', 'ticker', 'close_price']] 
                    df.columns = ['date', 'ticker', 'close_price']

                    if not df.empty: 
                        all_new_macro_data.append(df)
            except Exception as e:
                print(f"Error fetching {name} ({ticker_symbol}): {e}")

    if all_new_macro_data:
        final_df = pd.concat(all_new_macro_data).drop_duplicates()
        if not final_df.empty:
            final_df = final_df[['date', 'ticker', 'close_price']]
            final_df.columns = ['date', 'ticker', 'close_price']
            # print(f"Storing {len(final_df)} total new macro data points...") # Less verbose
            store_data_incrementally(final_df, 'macro_indicators', pk_columns_list=['date', 'ticker'])
    # else: # Less verbose
        # print("No new macro indicators fetched overall.")

def fetch_bitcoin_circulating_supply():
    print("Fetching Bitcoin circulating supply...")
    try:
        coin_data = cg.get_coin_by_id(id='bitcoin', market_data='true', community_data='false', 
                                      developer_data='false', localization='false')
        time.sleep(3) # Increased sleep for CoinGecko
        
        circulating_supply = None
        if coin_data and isinstance(coin_data, dict) and \
           'market_data' in coin_data and isinstance(coin_data['market_data'], dict) and \
           'circulating_supply' in coin_data['market_data']:
            circulating_supply = coin_data['market_data']['circulating_supply']
        
        if circulating_supply is not None:
            today_midnight_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            timestamp = int(today_midnight_utc.timestamp())
            
            df = pd.DataFrame([{'timestamp': timestamp, 'circulating_supply': circulating_supply}])
            store_data_incrementally(df, 'bitcoin_supply_info', pk_columns_list=['timestamp'])
            # print(f"Stored Bitcoin circulating supply: {circulating_supply} for {today_midnight_utc.strftime('%Y-%m-%d')}") # Less verbose
        else:
            print("Could not fetch Bitcoin circulating supply or response structure unexpected.")
            # print(f"DEBUG: Coin data for supply: {str(coin_data)[:500]}") 
    except Exception as e:
        print(f"Error fetching Bitcoin circulating supply: {e}")

def fetch_bitcoin_dominance(): 
    print("Fetching current Bitcoin dominance as a daily snapshot...")
    btc_mcap = None
    total_mcap = None
    try:
        # Fetch Bitcoin market data
        btc_response = cg.get_coin_by_id(id='bitcoin', market_data='true', sparkline='false',
                                         community_data='false', developer_data='false', localization='false')
        time.sleep(3) # Increased sleep for CoinGecko
        
        if btc_response and isinstance(btc_response, dict) and \
           'market_data' in btc_response and isinstance(btc_response['market_data'], dict) and \
           'market_cap' in btc_response['market_data'] and isinstance(btc_response['market_data']['market_cap'], dict) and \
           'usd' in btc_response['market_data']['market_cap']:
            btc_mcap = btc_response['market_data']['market_cap']['usd']
        else:
            print("Failed to get 'usd' market cap from btc_response or unexpected structure.")
            # print(f"DEBUG: BTC Response (type: {type(btc_response)}): {str(btc_response)[:500]}") # Keep for debugging if needed

        # Fetch global market data
        global_response = cg.get_global()
        time.sleep(3) # Increased sleep for CoinGecko

        # --- CORRECTED ACCESS TO total_market_cap ---
        if global_response and isinstance(global_response, dict) and \
           'total_market_cap' in global_response and isinstance(global_response['total_market_cap'], dict) and \
           'usd' in global_response['total_market_cap']:
            total_mcap = global_response['total_market_cap']['usd']
        else:
            print("Failed to get 'usd' total market cap from global_response or unexpected structure.")
            # You can keep or comment out the debug print once it's working
            print(f"DEBUG: Global Response (type: {type(global_response)}): {str(global_response)[:1000]}") 

        if btc_mcap is not None and total_mcap is not None and total_mcap > 0:
            dominance = (btc_mcap / total_mcap) * 100
            today_midnight_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            timestamp = int(today_midnight_utc.timestamp()) 
            
            df = pd.DataFrame([{'timestamp': timestamp, 'dominance': dominance}])
            store_data_incrementally(df, 'bitcoin_dominance', pk_columns_list=['timestamp'])
            # The print message from store_data_incrementally will confirm storage
        elif btc_mcap is not None and total_mcap is None: # Specifically if total_mcap is the issue
             print(f"Successfully fetched BTC MCap ({btc_mcap}), but Total MCap is still None. Check API response for 'total_market_cap'.")
        else:
            print(f"Could not calculate dominance. BTC MCap: {btc_mcap}, Total MCap: {total_mcap}")
            
    except Exception as e:
        print(f"Error fetching Bitcoin dominance snapshot: {e}")
        import traceback
        traceback.print_exc()

# --- MAIN EXECUTION (for direct testing of this file) ---
if __name__ == "__main__":
    init_db() 
    current_time_main = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time_main}] Starting data fetching tasks (direct run of data_fetcher.py)...")
    
    fetch_crypto_prices()
    fetch_fear_greed_index()
    fetch_google_trends()
    fetch_macro_indicators()
    fetch_bitcoin_circulating_supply() 
    fetch_bitcoin_dominance() 
    
    current_time_main = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time_main}] All data fetching tasks initiated (direct run).")
    print("NOTE: Calculated metrics (Pi Cycle, 200WMA, S2F, Puell) should be run AFTER this script via formulas.py or main.py")