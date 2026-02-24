import os
import pickle
import polars as pl
import pandas as pd
from datetime import datetime, date
from collections import defaultdict

# Paths to pipeline outputs
PRICE_PATH = os.path.join("data", "03_primary", "price_data.parquet")
FILING_PATH = os.path.join("data", "03_primary", "filing_data.parquet")
ALPACA_NEWS_PATH = os.path.join("data", "03_primary", "news.parquet")
# Google News: expects cleaned_google_{ticker}_2020-01-01_2024-12-31.csv in cwd or specify path
GOOGLE_NEWS_TEMPLATE = "cleaned_google_{ticker}_2020-01-01_2024-12-31.csv"

# Load price data
price_df = pl.read_parquet(PRICE_PATH).to_pandas()
# Load filing data
filing_df = pl.read_parquet(FILING_PATH).to_pandas()
# Load Alpaca news data
alpaca_news_df = pl.read_parquet(ALPACA_NEWS_PATH).to_pandas()

# Get all tickers
all_tickers = price_df["equity"].unique().tolist()

# Build env_data_pkl
env_data_pkl = defaultdict(
    lambda: {"price": {}, "filing_k": {}, "filing_q": {}, "news": {}}
)

# Fill price
for _, row in price_df.iterrows():
    d = pd.to_datetime(row["est_time"]).date()
    env_data_pkl[d]["price"][row["equity"]] = row["close"]

# Fill filings (10-K, 10-Q)
for _, row in filing_df.iterrows():
    d = pd.to_datetime(row["date"]).date()
    ticker = row["ticker"]
    form = row["form"]
    content = row["content"]
    if form == "10-K":
        env_data_pkl[d]["filing_k"][ticker] = content
    elif form == "10-Q":
        env_data_pkl[d]["filing_q"][ticker] = content

# Fill Alpaca news
for _, row in alpaca_news_df.iterrows():
    d = (
        pd.to_datetime(row["date"]).date()
        if "date" in row
        else pd.to_datetime(row["datetime"]).date()
    )
    ticker = row["equity"]
    headline = row.get("title", "")
    summary = row.get("summary", "")
    news_item = f"{headline} {summary}".strip()
    if news_item:
        env_data_pkl[d]["news"].setdefault(ticker, []).append(news_item)

# Fill Google news (if available)
for ticker in all_tickers:
    google_news_path = GOOGLE_NEWS_TEMPLATE.format(ticker=ticker)
    if os.path.exists(google_news_path):
        google_df = pd.read_csv(google_news_path)
        for _, row in google_df.iterrows():
            d = pd.to_datetime(row["date"]).date()
            news_item = row.get("headline", "") + " " + row.get("body", "")
            news_item = news_item.strip()
            if news_item:
                env_data_pkl[d]["news"].setdefault(ticker, []).append(news_item)

# Convert defaultdict to dict
env_data_pkl = dict(env_data_pkl)

# Save to pickle
with open("env_data_pkl.pkl", "wb") as f:
    pickle.dump(env_data_pkl, f)

print(f"env_data_pkl.pkl generated with {len(env_data_pkl)} dates.")
