# Crypto Market Cycle Top Detection Dashboard

This project is a Python-based dashboard designed to monitor key on-chain and market metrics that have historically signaled Bitcoin bull cycle tops. It uses Streamlit for the frontend and fetches data from various free public APIs.

## Features

* Displays multiple crypto market indicators.
* Color-coded risk signals (Green/Yellow/Red) based on configurable thresholds.
* Calculates several derived metrics like Pi Cycle Top, 200WMA, S2F Model deviation, and Puell Multiple.
* Includes market sentiment indicators like Fear & Greed Index and Google Trends.
* Tracks relevant macro indicators.

## Tech Stack

* **Backend & Data Fetching:** Python, Pandas, Requests, PyCoinGecko, yfinance, Pytrends
* **Calculations:** Python, Pandas
* **Database:** SQLite
* **Frontend:** Streamlit, Plotly
* **Scheduling (Local):** Python `schedule` library

## Setup & Installation

1.  **Clone the repository (if you haven't already):**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/YOUR_REPOSITORY_NAME.git](https://github.com/YOUR_USERNAME/YOUR_REPOSITORY_NAME.git)
    cd YOUR_REPOSITORY_NAME
    ```
2.  **Create and activate a Python virtual environment:**
    ```bash
    python -m venv venv
    # On Windows:
    .\venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Initial Data Fetch:**
    Before running the dashboard for the first time, or if the `data/crypto_metrics.db` file is missing, run the main data fetching and calculation script once to populate the database with historical data:
    ```bash
    python main.py
    ```
    This initial run might take several minutes. Subsequent runs of `main.py` will perform incremental updates.

## Running the Dashboard

1.  **Ensure your virtual environment is activated**
2.  **If you want automated daily data updates (runs at 1:00 AM by default):**
    Keep the `main.py` script running in a terminal:
    ```bash
    python main.py
    ```
3.  **To view the dashboard:**
    Open another terminal, activate the virtual environment, navigate to the project directory, and run:
    ```bash
    streamlit run dashboard.py
    ```
    This will open the dashboard in your web browser. The dashboard reads from the SQLite database, which is updated by `main.py` or by the "Fetch Latest Data" button within the dashboard.

## Indicators Included (Initial Version)

* Bitcoin & Ethereum Price
* Fear & Greed Index
* Google Trends for "Bitcoin"
* Pi Cycle Top Indicator
* Bitcoin Price vs. 200-Week Moving Average (200WMA)
* Bitcoin Dominance
* Stock-to-Flow (S2F) Model Price Deviation
* Calculated Puell Multiple
* Macro Indicators (S&P 500, Gold, DXY, US10Y)

## Configuration

* Risk thresholds for indicators can be configured in `thresholds_config.py`.
* Data fetching schedule is in `main.py`.

## Disclaimer

This dashboard is for informational and educational purposes only. It is not financial advice. Always do your own research before making any investment decisions. The accuracy of the data depends on the free public APIs used.