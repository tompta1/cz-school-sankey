from __future__ import annotations

import requests

from etl.environment import _common


class FakeResponse:
    content = b"downloaded"

    def raise_for_status(self) -> None:
        return None


def test_fetch_bytes_retries_transient_request_errors(monkeypatch) -> None:
    attempts = 0
    sleeps: list[int] = []

    def fake_get(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise requests.ConnectionError("temporary failure")
        return FakeResponse()

    monkeypatch.setattr(_common.requests, "get", fake_get)
    monkeypatch.setattr(_common.time, "sleep", sleeps.append)

    assert _common.fetch_bytes("https://example.test/data") == b"downloaded"
    assert attempts == 3
    assert sleeps == [1, 2]


def test_fetch_bytes_raises_after_last_attempt(monkeypatch) -> None:
    def fake_get(*args, **kwargs):
        raise requests.ConnectionError("still unavailable")

    monkeypatch.setattr(_common.requests, "get", fake_get)
    monkeypatch.setattr(_common.time, "sleep", lambda _: None)

    try:
        _common.fetch_bytes("https://example.test/data", attempts=2)
    except requests.ConnectionError as error:
        assert str(error) == "still unavailable"
    else:
        raise AssertionError("expected the final request error")
