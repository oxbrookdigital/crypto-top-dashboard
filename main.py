# main.py
import schedule
import time
import datetime # For logging timestamp with full module name
import data_fetcher 
import formulas   

def run_daily_tasks():
    current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time_str}] Starting daily data update and calculations...")
    
    try:
        # Step 1: Fetch new data from external APIs (incrementally)
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Running data_fetcher tasks ---")
        
        data_fetcher.fetch_crypto_prices()
        data_fetcher.fetch_fear_greed_index()
        data_fetcher.fetch_google_trends()
        data_fetcher.fetch_macro_indicators()
        data_fetcher.fetch_bitcoin_circulating_supply() # Fetch supply needed for S2F
        data_fetcher.fetch_bitcoin_dominance() 

        # Step 2: Calculate derived metrics using the latest data in the DB
        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Data fetching complete. Running formula calculations ---")
        formulas.calculate_pi_cycle_top()
        formulas.calculate_200wma()
        formulas.calculate_s2f_model()
        formulas.calculate_puell_multiple_alternative()
        
        current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time_str}] --- Daily tasks finished successfully. ---")
        
    except Exception as e:
        current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time_str}] Error during scheduled daily tasks: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging errors in tasks

if __name__ == "__main__":
    print("Initializing database schema if it doesn't exist...")
    data_fetcher.init_db() 
    
    current_time_main_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time_main_str}] Scheduler started.")
    
    # Option 1: Run once immediately on start, then schedule
    print("An initial data fetch and calculation cycle will run now.")
    run_daily_tasks() 
    
    # Option 2: Just schedule it (it will run at the next scheduled time)
    # print("Tasks scheduled. Will run at the configured time.")

    schedule.every().day.at("01:00").do(run_daily_tasks) 
    current_time_main_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time_main_str}] Daily tasks scheduled. Next run for most tasks is around: {schedule.next_run()}")

    while True:
        schedule.run_pending()
        time.sleep(30)