import os
import polars as pl
import pandas as pd
import yfinance as yf

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
        df = yf.Ticker(ticker).history(start=START_DATE, end=END_DATE)
        df = df.reset_index()
        if df.shape[0] == 0 or "Close" not in df.columns:
            print(f"Warning: No valid price data for {ticker}")
            continue
        for idx, row in df.iterrows():
            est_time = row["Date"]
            close = row["Close"]
            if pd.isna(est_time) or pd.isna(close):
                continue
            # Normalize timezone-aware datetimes to naive date string
            if hasattr(est_time, "tz_localize"):
                est_time = est_time.tz_localize(None) if est_time.tzinfo else est_time
            records.append(
                {
                    "est_time": (
                        pd.Timestamp(est_time).tz_localize(None)
                        if pd.Timestamp(est_time).tzinfo
                        else pd.Timestamp(est_time)
                    ),
                    "equity": ticker,
                    "close": float(close),
                }
            )
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

price_df = pd.DataFrame(records)
price_pl = pl.from_pandas(price_df)
os.makedirs("data/03_primary", exist_ok=True)
price_pl.write_parquet("data/03_primary/price_data.parquet")
print(f"Saved price_data.parquet with {len(price_df)} rows.")
