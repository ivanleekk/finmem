import os
import pickle
import polars as pl
import pandas as pd
from collections import defaultdict

# Paths to pipeline outputs
PRICE_PATH = os.path.join("data", "03_primary", "price_data.parquet")
FILING_PATH = os.path.join("data", "03_primary", "filing_data.parquet")
ALPACA_NEWS_PATH = os.path.join("data", "03_primary", "news.parquet")
# Google News: expects cleaned_google_{ticker}_2020-01-01_2024-12-31.csv in cwd
GOOGLE_NEWS_TEMPLATE = "cleaned_google_{ticker}_2020-01-01_2024-12-31.csv"
OUTPUT_DIR = os.path.join("data", "03_model_input")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load source data
price_df = pl.read_parquet(PRICE_PATH).to_pandas()
filing_df = pl.read_parquet(FILING_PATH).to_pandas()

alpaca_available = os.path.exists(ALPACA_NEWS_PATH)
if alpaca_available:
    alpaca_news_df = pl.read_parquet(ALPACA_NEWS_PATH).to_pandas()
    # Normalise date column
    if "date" in alpaca_news_df.columns:
        alpaca_news_df["_date"] = pd.to_datetime(alpaca_news_df["date"]).dt.date
    else:
        alpaca_news_df["_date"] = pd.to_datetime(alpaca_news_df["datetime"]).dt.date

all_tickers = price_df["equity"].unique().tolist()

for ticker in all_tickers:
    print(f"Building env_data_pkl for {ticker}...")
    env_data_pkl = defaultdict(
        lambda: {"price": {}, "filing_k": {}, "filing_q": {}, "news": {}}
    )

    # Price
    for _, row in price_df[price_df["equity"] == ticker].iterrows():
        d = pd.to_datetime(row["est_time"]).date()
        env_data_pkl[d]["price"][ticker] = float(row["close"])

    # Filings
    ticker_filings = filing_df[filing_df["ticker"] == ticker]
    for _, row in ticker_filings.iterrows():
        d = pd.to_datetime(row["date"]).date()
        if row["form"] == "10-K":
            env_data_pkl[d]["filing_k"][ticker] = row["content"]
        elif row["form"] in ("10-Q", "20-F"):
            env_data_pkl[d]["filing_q"][ticker] = row["content"]

    # Alpaca news
    if alpaca_available:
        ticker_news = alpaca_news_df[alpaca_news_df["equity"] == ticker]
        for _, row in ticker_news.iterrows():
            d = row["_date"]
            headline = row.get("title", "") or ""
            summary = row.get("summary", "") or ""
            news_item = f"{headline} {summary}".strip()
            if news_item:
                env_data_pkl[d]["news"].setdefault(ticker, []).append(news_item)

    # Google news
    google_news_path = GOOGLE_NEWS_TEMPLATE.format(ticker=ticker)
    if os.path.exists(google_news_path):
        google_df = pd.read_csv(google_news_path)
        for _, row in google_df.iterrows():
            d = pd.to_datetime(row["date"]).date()
            news_item = (
                str(row.get("headline", "")) + " " + str(row.get("body", ""))
            ).strip()
            if news_item:
                env_data_pkl[d]["news"].setdefault(ticker, []).append(news_item)

    env_data_pkl = dict(env_data_pkl)

    # Save one pkl per ticker
    safe_ticker = ticker.replace("/", "_").replace(".", "_")
    out_path = os.path.join(OUTPUT_DIR, f"env_data_{safe_ticker}.pkl")
    with open(out_path, "wb") as f:
        pickle.dump(env_data_pkl, f)
    print(f"  Saved {out_path} with {len(env_data_pkl)} dates.")

print("Done — all per-ticker pkl files written to", OUTPUT_DIR)
