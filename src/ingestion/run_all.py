from src.ingestion import brent, calendar_features, weather
from src.ingestion.common import get_logger

log = get_logger("run_all")

# trends, hcp and fx are run separately (rate limits / manual download)
JOBS = {"weather": weather.run, "brent": brent.run, "calendar": calendar_features.run}

if __name__ == "__main__":
    failures = []
    for name, fn in JOBS.items():
        try:
            fn()
        except Exception as exc:
            log.error("job %s failed: %s", name, type(exc).__name__)
            failures.append(name)
    if failures:
        raise SystemExit(f"failed jobs: {failures}")
