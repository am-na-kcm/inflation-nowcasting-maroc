import responses

from src.ingestion.common import http_get


@responses.activate
def test_http_get_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    responses.add(responses.GET, "https://x.test/a", status=500)
    responses.add(responses.GET, "https://x.test/a", json={"ok": True}, status=200)
    assert http_get("https://x.test/a").json() == {"ok": True}
    assert len(responses.calls) == 2
