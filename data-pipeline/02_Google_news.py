import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import random
import re
import requests
import warnings
from newspaper import Article  # pip install newspaper3k
from cleantext import clean
from Levenshtein import ratio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result

# Suppress warnings from yfinance and BeautifulSoup for cleaner output
warnings.filterwarnings("ignore")

# ==========================================
# 1. SCRAPING FUNCTIONS
# ==========================================


def get_full_text(url):
    """Downloads and parses the full article content from a URL."""
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception:
        # Silently fail and return empty string if newspaper3k gets blocked
        return ""


def get_google_news_full(query, start_date, end_date):
    """
    Scrapes Google News, fetching snippets and attempting to grab the full article text.
    Returns a DataFrame with columns: ['dates', 'headline', 'body', 'link']
    """
    d1 = datetime.strptime(start_date, "%Y-%m-%d").strftime("%m/%d/%Y")
    d2 = datetime.strptime(end_date, "%Y-%m-%d").strftime("%m/%d/%Y")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/101.0.4951.54 Safari/537.36"
    }
    news_results = []

    print(f"Scraping Google News for {query} from {start_date} to {end_date}...")

    # Limit to 5 pages (approx 50 results) to avoid heavy rate limiting
    for page in range(5):
        offset = page * 10
        url = f"https://www.google.com/search?q={query}&tbs=cdr:1,cd_min:{d1},cd_max:{d2}&tbm=nws&start={offset}"

        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, "html.parser")
            results = soup.select("div.SoaBEf")

            if not results:
                break

            for el in results:
                try:
                    link = el.find("a")["href"]
                    headline = el.select_one("div.MBeuO").get_text()

                    # Try to get full text, fallback to Google snippet
                    full_body = get_full_text(link)
                    body_text = (
                        full_body if full_body else el.select_one(".GI74Re").get_text()
                    )

                    # We use a static timestamp to simulate Refinitiv's format so the pipeline doesn't break.
                    # In a production app, you would parse Google's relative dates ("3 days ago")
                    simulated_date = f"{start_date} 12:00:00"

                    news_results.append(
                        {
                            "dates": simulated_date,
                            "headline": headline,
                            "body": body_text,
                        }
                    )
                except Exception:
                    continue

                time.sleep(random.uniform(0.5, 2))  # Be polite to the servers

            if not soup.find("a", id="pnnext"):
                break

            print(f"Scraped page {page + 1}...")

        except Exception as e:
            print(f"Search Error: {e}")
            break

    return pd.DataFrame(news_results)


# ==========================================
# 2. REFINITIV PROCESSING FUNCTIONS
# ==========================================


def extract_update_number(headline):
    match = re.match(r"UPDATE (\d+)", headline)
    return int(match.group(1)) if match else 0


def create_new_headline(row):
    if pd.notnull(row["update_number"]):
        return re.sub(r"UPDATE \d+-", "", row["headline"])
    else:
        return row["headline"]


def clean_news(df):
    for index, row in df.iterrows():
        if isinstance(row["body"], str) and "(Reuters)" in row["body"]:
            position = row["body"].find("(Reuters)")
            df.at[index, "body"] = row["body"][position:]
    return df


def remove_spaces(df, column_names):
    for column in column_names:
        temp_column = f"temp_{column}"
        # Cast to string first to avoid errors on empty bodies
        df[temp_column] = df[column].astype(str).str.replace(" ", "", regex=True)
    return df


def replace_column_values(df, column_name, new_value):
    df[column_name] = new_value
    return df


def calculate_date(row):
    if row.hour >= 16:
        return (row + timedelta(days=1)).date()
    else:
        return row.date()


def clean_text(text):
    if not isinstance(text, str):
        return ""
    return clean(
        text,
        fix_unicode=True,
        to_ascii=True,
        lower=True,
        no_line_breaks=True,
        no_urls=True,
        no_emails=True,
        no_phone_numbers=True,
        no_numbers=False,
        no_digits=False,
        no_currency_symbols=False,
        no_punct=False,
        lang="en",
    )


def drop_similar_records(df, column_name, r):
    if df.empty:
        return df

    df[column_name] = df[column_name].astype(str)
    df["drop_row"] = False

    # Reset index to ensure sequential iteration
    df = df.reset_index(drop=True)

    for i in range(1, len(df)):
        similarity = ratio(df.at[i, column_name], df.at[i - 1, column_name])
        if similarity > r:
            df.at[i, "drop_row"] = True

    df = df[~df["drop_row"]].drop("drop_row", axis=1)
    return df


def adjust_trading_days(start_day, end_day, ticker, df):
    # Expand the yfinance window slightly to ensure we catch the next trading day
    yf_start = (datetime.strptime(start_day, "%Y-%m-%d") - timedelta(days=5)).strftime(
        "%Y-%m-%d"
    )
    yf_end = (datetime.strptime(end_day, "%Y-%m-%d") + timedelta(days=10)).strftime(
        "%Y-%m-%d"
    )

    df_yf = yf.download(ticker, start=yf_start, end=yf_end, progress=False)
    if df_yf.empty:
        print(f"Warning: No YFinance data found for {ticker} in this range.")
        return df

    df_yf = df_yf.reset_index()
    df_yf["Date"] = pd.to_datetime(df_yf["Date"]).dt.date

    df["date"] = pd.to_datetime(df["date"]).dt.date
    yf_date = df_yf["Date"].tolist()

    trading = []
    for day in df["date"]:
        # Prevent infinite loop if we go beyond available YF data
        loop_count = 0
        while day not in yf_date and loop_count < 10:
            day += timedelta(days=1)
            loop_count += 1
        trading.append(day)

    df["date"] = trading
    return df


# ==========================================
# 3. MAIN PIPELINE INTEGRATION
# ==========================================


def process_pipeline(df, ticker, save_path, start_day, end_day):
    """
    Pushes the scraped DataFrame through the Refinitiv cleaning pipeline.
    """
    if df.empty:
        print(f"No data to process for {ticker}.")
        return

    original_length = len(df)

    df["update_number"] = df["headline"].apply(extract_update_number)
    df["new_headline"] = df.apply(create_new_headline, axis=1)
    df_sorted = df.sort_values(by="update_number", ascending=False)

    df_drop_update = df_sorted.drop_duplicates(subset=["new_headline"], keep="first")
    df_drop_update = df_drop_update.sort_values(by="dates", ascending=False)
    df_drop_reuters = clean_news(df_drop_update)
    df_remove_spaces = remove_spaces(df_drop_reuters, ["new_headline", "body"])

    df_cleaned = df_remove_spaces.drop_duplicates(subset=["temp_body"], keep="first")
    df_cleaned = df_cleaned.drop_duplicates(subset=["temp_new_headline"], keep="first")
    df_final = df_cleaned.sort_values(by="dates")

    # Add symbol column
    replace_column_values(df_final, "symbols", ticker)

    # Handle dates
    df_final["dates"] = df_final["dates"].str[:19]
    df_final["dates"] = pd.to_datetime(df_final["dates"])
    df_final["date"] = df_final["dates"].apply(calculate_date)

    # Clean text body
    df_final["cleaned_body"] = df_final["body"].apply(clean_text)
    df_final = df_final.drop_duplicates(subset=["cleaned_body"], keep="first")

    # Clean up columns before Levenshtein
    columns_to_drop = [
        "update_number",
        "headline",
        "temp_body",
        "temp_new_headline",
        "body",
        "dates",
    ]
    df_final = df_final.drop(
        columns=[col for col in columns_to_drop if col in df_final.columns]
    )

    # Save to disk as an intermediary step (from original script logic)
    df_final.to_csv(save_path, index=False)

    # Reload and run Levenshtein on body (Threshold: 0.6)
    df_similar = pd.read_csv(save_path)
    df_drop_similar = drop_similar_records(df_similar, "cleaned_body", 0.6)
    df_drop_similar.to_csv(save_path, index=False)

    # Reload and run Levenshtein on headline (Threshold: 0.9)
    df_similar = pd.read_csv(save_path)
    df_drop_similar = drop_similar_records(df_similar, "new_headline", 0.9)

    # Rename and reorder
    df_drop_similar = df_drop_similar[
        ["date", "symbols", "new_headline", "cleaned_body"]
    ]
    df_drop_similar = df_drop_similar.rename(
        columns={"new_headline": "headline", "cleaned_body": "body"}
    )

    # Map to trading days
    adjusted_df = adjust_trading_days(start_day, end_day, ticker, df_drop_similar)
    adjusted_df.to_csv(save_path, index=False)

    print(f"{ticker} before cleaned:", original_length)
    print(f"{ticker} after cleaned:", len(adjusted_df))


def fetch_news_weekly(ticker, start_day, end_day):
    """
    Loops through the date range week by week, calling the Google News scraper
    for each week to build a dense dataset over long time periods.
    """
    start_date = datetime.strptime(start_day, "%Y-%m-%d")
    end_date = datetime.strptime(end_day, "%Y-%m-%d")

    current_date = start_date
    all_news_dfs = []

    # Calculate total weeks for the progress tracker
    total_weeks = (end_date - start_date).days // 7 + 1
    week_count = 1

    while current_date <= end_date:
        # Define the end of the current week (6 days later)
        week_end = current_date + timedelta(days=6)

        # Don't overshoot the final end date
        if week_end > end_date:
            week_end = end_date

        str_start = current_date.strftime("%Y-%m-%d")
        str_end = week_end.strftime("%Y-%m-%d")

        print(f"  [{week_count}/{total_weeks}] Fetching week: {str_start} to {str_end}")

        # Scrape for this specific week
        # NOTE: Because we are searching weekly, getting just 1 or 2 pages per week
        # is usually enough to capture all the major news and prevents immediate IP bans.
        df_week = get_google_news_full(ticker, str_start, str_end)

        if df_week is not None and not df_week.empty:
            all_news_dfs.append(df_week)

        # VERY IMPORTANT: Rest between weekly requests to avoid Google CAPTCHA bans
        time.sleep(random.uniform(3, 7))

        # Move to the start of the next week
        current_date = week_end + timedelta(days=1)
        week_count += 1

    if all_news_dfs:
        # Combine all weekly dataframes into one large dataframe
        return pd.concat(all_news_dfs, ignore_index=True)
    else:
        return pd.DataFrame()


if __name__ == "__main__":
    # Parameters
    TIER_1_TICKERS = [
        # US - Tech
        "NVDA",
        "AAPL",
        "MSFT",
        "AVGO",
        # US - Healthcare
        "LLY",
        "JNJ",
        "ABBV",
        "UNH",
        # US - Financial
        "BRK-B",
        "JPM",
        "V",
        "BAC",
        # US - Consumer Cyclical
        "AMZN",
        "TSLA",
        "HD",
        "MCD",
        # US - Consumer Defensive
        "WMT",
        "COST",
        "PG",
        "KO",
        # SGX
        "D05.SI",
        "O39.SI",
        "Z74.SI",
        "U11.SI",
        # HKEX
        "0700.HK",
        "9988.HK",
        "1398.HK",
        "1288.HK",
        # LSE
        "AZN.L",
        "HSBA.L",
        "SHEL.L",
        "RR.L",
    ]

    START_DAY = "2020-01-01"
    END_DAY = "2024-12-31"

    print(f"Starting pipeline for {len(TIER_1_TICKERS)} tickers...")

    for ticker in TIER_1_TICKERS:
        try:
            print(f"\n{'='*50}")
            print(f"Processing Ticker: {ticker}")
            print(f"{'='*50}")

            SAVE_PATH = f"cleaned_google_{ticker}_{START_DAY}_{END_DAY}.csv"

            # 1. Scrape data using the WEEKLY loop
            raw_df = fetch_news_weekly(ticker, START_DAY, END_DAY)

            # 2. Push through the cleaning and alignment pipeline
            if raw_df is not None and not raw_df.empty:
                print(f"Total raw articles fetched for {ticker}: {len(raw_df)}")
                process_pipeline(raw_df, ticker, SAVE_PATH, START_DAY, END_DAY)
            else:
                print(f"No news data found for {ticker} in this time period.")

            # IMPORTANT: Deep sleep between tickers to reset rate limiters
            sleep_time = random.uniform(15, 30)
            print(f"Resting for {sleep_time:.2f} seconds before the next ticker...")
            time.sleep(sleep_time)

        except Exception as e:
            print(f"An error occurred while processing {ticker}: {e}")
            continue

    print("\nAll tier 1 tickers processed!")
