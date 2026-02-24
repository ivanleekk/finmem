import os
import polars as pl
import pandas as pd
import yfinance as yf
from datetime import datetime

# List of all tickers
TICKERS = [
    "NVDA",
    "AAPL",
    "MSFT",
    "AVGO",
    "LLY",
    "JNJ",
    "ABBV",
    "UNH",
    "BRK-B",
    "JPM",
    "V",
    "BAC",
    "AMZN",
    "TSLA",
    "HD",
    "MCD",
    "WMT",
    "COST",
    "PG",
    "KO",
    "D05.SI",
    "O39.SI",
    "Z74.SI",
    "U11.SI",
    "0700.HK",
    "9988.HK",
    "1398.HK",
    "1288.HK",
    "AZN.L",
    "HSBA.L",
    "SHEL.L",
    "RR.L",
]

START_DATE = "2020-01-01"
END_DATE = "2024-12-31"

records = []

for ticker in TICKERS:
    print(f"Fetching price for {ticker}...")
    try:
        df = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)
        if df.empty:
            print(f"Warning: No data for {ticker}")
            continue
        df = df.reset_index()
        for _, row in df.iterrows():
            records.append(
                {"est_time": row["Date"], "equity": ticker, "close": row["Close"]}
            )
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

price_df = pd.DataFrame(records)
price_pl = pl.from_pandas(price_df)
os.makedirs("data/03_primary", exist_ok=True)
price_pl.write_parquet("data/03_primary/price_data.parquet")
print(f"Saved price_data.parquet with {len(price_df)} rows.")
