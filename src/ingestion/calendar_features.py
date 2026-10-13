from datetime import date, timedelta

import pandas as pd
from hijridate import Hijri

from src.ingestion.common import landing_path


def to_date(g):
    return date(g.year, g.month, g.day)


def build(h_start=1435, h_end=1449):
    ramadans, eids = [], []
    for hy in range(h_start, h_end + 1):
        start = to_date(Hijri(hy, 9, 1).to_gregorian())
        end = to_date(Hijri(hy, 10, 1).to_gregorian()) - timedelta(days=1)
        ramadans.append((start, end))
        eids.append(to_date(Hijri(hy, 12, 10).to_gregorian()))
    days = pd.DataFrame({"date": pd.date_range("2014-01-01", "2028-12-31", freq="D")})
    days["is_ramadan"] = days["date"].apply(lambda d: any(s <= d.date() <= e for s, e in ramadans))
    days["month"] = days["date"].dt.to_period("M").dt.to_timestamp()
    out = days.groupby("month")["is_ramadan"].mean().rename("ramadan_share").reset_index()
    eid_months = {pd.Timestamp(d.year, d.month, 1) for d in eids}
    out["eid_adha_month"] = out["month"].isin(eid_months).astype(int)
    return out


def run():
    build().to_csv(landing_path("calendar", "calendar_features.csv"), index=False)


if __name__ == "__main__":
    run()
