import os

from dotenv import load_dotenv

from src.ingestion.common import http_get, save_json

load_dotenv()
URL = "https://api.stlouisfed.org/fred/series/observations"


def run(series="DCOILBRENTEU", start="2015-01-01"):
    params = {
        "series_id": series,
        "api_key": os.environ["FRED_API_KEY"],
        "file_type": "json",
        "observation_start": start,
    }
    payload = http_get(URL, params).json()
    save_json("brent", f"{series}.json", payload)


if __name__ == "__main__":
    run()
