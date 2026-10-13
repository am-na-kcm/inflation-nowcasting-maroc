from src.ingestion.calendar_features import build


def test_ramadan_march_2025():
    df = build()
    row = df[df["month"] == "2025-03-01"].iloc[0]
    assert row["ramadan_share"] > 0.8


def test_one_row_per_month():
    df = build()
    assert df["month"].is_unique
