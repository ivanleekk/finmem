import polars as pl
from edgar import set_identity, Company
from tqdm import tqdm

set_identity("Your Name yourname@example.com")

# Define your list
STOCKS = {
    "US": [
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
    ],
    "INTL": [
        "D05.SI",
        "O39.SI",
        "Z74.SI",
        "U11.SI",  # SGX
        "0700.HK",
        "9988.HK",
        "1398.HK",
        "1288.HK",  # HKEX
        "AZN.L",
        "HSBA.L",
        "SHEL.L",
        "RR.L",
    ],  # LSE
}


def get_global_filings(stock_dict):
    results = []

    # Flatten list for processing
    all_tickers = stock_dict["US"] + stock_dict["INTL"]

    for ticker in tqdm(all_tickers):
        try:
            # For INTL stocks, we try to find their US-equivalent ticker if available
            # e.g., 0700.HK -> TCEHY (Tencent ADR)
            company = Company(ticker)

            # We look for 10-K/Q (Domestic) and 20-F (Foreign Annual)
            filings = company.get_filings(form=["10-K", "10-Q", "20-F"]).filter(
                date="2020-01-01:"
            )

            for filing in filings:
                doc = filing.obj()
                content = ""

                if filing.form == "10-K":
                    content = getattr(doc, "management_discussion", "")
                elif filing.form == "10-Q":
                    content = getattr(doc, "mda", "")
                elif filing.form == "20-F":
                    # For foreign firms, MD&A is usually 'Item 5'
                    content = getattr(doc, "item5", "")

                if content:
                    results.append(
                        {
                            "ticker": ticker,
                            "form": filing.form,
                            "date": filing.filing_date,
                            "content": str(content)[:10000],  # Save first 10k chars
                        }
                    )

        except Exception:
            # This will happen if the stock (like DBS) isn't registered with the US SEC
            continue

    return pl.DataFrame(results)


if __name__ == "__main__":
    final_df = get_global_filings(STOCKS)
    final_df.write_parquet("global_filing_data.parquet")
