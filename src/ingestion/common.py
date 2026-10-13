import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

LANDING = Path("data/landing")


def get_logger(name):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return logging.getLogger(name)


log = get_logger("common")


def http_get(url, params=None, retries=5, backoff=2.0, timeout=30):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"status {r.status_code}")
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            wait = backoff**attempt
            # do not log str(exc): it may contain the URL and thus the API key
            log.warning(
                "attempt %s/%s failed (%s), retry in %.0fs",
                attempt,
                retries,
                type(exc).__name__,
                wait,
            )
            if attempt == retries:
                raise
            time.sleep(wait)


def landing_path(source, filename):
    now = datetime.now(timezone.utc)
    folder = LANDING / source / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / filename


def save_json(source, filename, payload):
    path = landing_path(source, filename)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    log.info("saved %s", path)
    return path
