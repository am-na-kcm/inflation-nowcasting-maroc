import random
import time

from pytrends.request import TrendReq

from src.ingestion.common import get_logger, landing_path

log = get_logger("trends")
KEYWORDS = [
    "prix huile",
    "prix tomate",
    "prix poulet",
    "prix viande",
    "prix mouton",
    "prix gasoil",
    "prix essence",
    "loyer",
    "prix ciment",
]


def fetch_once(keyword, timeframe, retries=4):
    for attempt in range(1, retries + 1):
        try:
            pt = TrendReq(hl="fr-FR", tz=0, timeout=(10, 30))
            pt.build_payload([keyword], geo="MA", timeframe=timeframe)
            df = pt.interest_over_time()
            if df.empty:
                raise ValueError("empty response")
            df = df.reset_index()[["date", keyword]]
            return df.rename(columns={keyword: "value"})
        except Exception as exc:  # pytrends raises several exception types
            wait = 60 * attempt
            log.warning(
                "trends %s attempt %s failed (%s), wait %ss",
                keyword,
                attempt,
                type(exc).__name__,
                wait,
            )
            if attempt == retries:
                raise
            time.sleep(wait)


def run(timeframe="2015-01-01 2025-12-31", n_draws=3):
    for kw in KEYWORDS:
        for draw in range(1, n_draws + 1):
            df = fetch_once(kw, timeframe)
            df["keyword"] = kw
            df["draw"] = draw
            slug = kw.replace(" ", "_")
            df.to_csv(landing_path("trends", f"{slug}_draw{draw}.csv"), index=False)
            time.sleep(random.uniform(20, 40))  # be gentle with Google


if __name__ == "__main__":
    run()
