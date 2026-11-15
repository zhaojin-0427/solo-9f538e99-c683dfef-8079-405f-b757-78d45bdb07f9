"""Tests for pathological within-limit uploads (bounded-memory validation).

Covers the incident scenario: a ~4 MB CSV with nearly a million duplicate
hourly rows must be fully scanned and rejected with a clean 422 — capped
per-row details, exact per-type totals, and the complete missing-hour list —
inside a 256 MiB memory budget, without blocking concurrent queries.
"""
from __future__ import annotations

import io
import os
import subprocess
import sys
import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from conftest import full_csv

ROOT = Path(__file__).resolve().parent.parent


def _upload(client, station, day, raw, expected_version=0):
    return client.post(
        f"/stations/{station}/dates/{day}/uploads",
        data={"expected_version": str(expected_version)},
        files={"file": ("data.csv", io.BytesIO(raw), "text/csv")},
    )


# ---------------------------------------------------------------------------
# Unit level: bounded details, exact totals, complete missing hours
# ---------------------------------------------------------------------------

def test_duplicate_flood_details_capped_counts_complete():
    from app.validation import CsvValidationError, parse_and_validate

    n = 50_000
    raw = b"hour,pm25\n" + b"5,12.3\n" * n
    with pytest.raises(CsvValidationError) as exc:
        parse_and_validate(raw)
    err = exc.value

    # Only the first 100 per-row errors are kept verbatim, in file order.
    assert len(err.reasons) == 100
    assert err.reasons[0] == {
        "reason": "duplicate hour", "line": 3, "hour": 5, "first_line": 2,
    }
    assert err.reasons[-1]["line"] == 102
    assert err.details_truncated

    # Totals per error type are exact, and every missing hour is reported.
    assert err.error_counts == {"duplicate hour": n - 1, "missing hour": 23}
    assert err.total_errors == (n - 1) + 23
    assert err.missing_hours == [h for h in range(24) if h != 5]


def test_small_file_still_reports_every_error_detail():
    from app.validation import CsvValidationError, parse_and_validate

    rows = [(h, 1.0) for h in range(24) if h not in (5, 17)]
    rows = [(h, -2.0 if h == 7 else v) for h, v in rows]  # hour 7 also drops out
    raw = ("hour,pm25\n" + "\n".join(f"{h},{v}" for h, v in rows) + "\n").encode()
    with pytest.raises(CsvValidationError) as exc:
        parse_and_validate(raw)
    err = exc.value

    assert not err.details_truncated
    assert err.total_errors == len(err.reasons) == 4
    assert err.error_counts == {
        "pm25 concentration must be non-negative": 1,
        "missing hour": 3,
    }
    assert err.missing_hours == [5, 7, 17]
    # Per-row detail is as rich as before the fix.
    neg = next(r for r in err.reasons if "non-negative" in r["reason"])
    assert neg["value"] == -2.0 and neg["line"] == 8


def test_oversized_cell_value_is_clipped_in_details():
    from app.validation import CsvValidationError, parse_and_validate

    huge = "9" * 5000  # parses to inf -> error entry echoes the raw text
    rows = [(h, 1.0) for h in range(23)]
    raw = ("hour,pm25\n" + "\n".join(f"{h},{v}" for h, v in rows)
           + f"\n23,{huge}\n").encode()
    with pytest.raises(CsvValidationError) as exc:
        parse_and_validate(raw)
    entry = next(r for r in exc.value.reasons if "finite" in r["reason"])
    assert len(entry["value"]) <= 129  # 128 chars + ellipsis


def test_wide_row_is_clipped_in_details():
    from app.validation import CsvValidationError, parse_and_validate

    wide = ",".join(["1"] * 50)
    rows = "\n".join(f"{h},1.0" for h in range(24))
    raw = f"hour,pm25\n{wide}\n{rows}\n".encode()
    with pytest.raises(CsvValidationError) as exc:
        parse_and_validate(raw)
    entry = next(r for r in exc.value.reasons if "columns" in r["reason"])
    assert len(entry["row"]) == 9  # 8 kept fields + truncation marker
    assert "42 more fields" in entry["row"][-1]


@pytest.mark.skipif(not hasattr(os, "getuid"), reason="POSIX-only test")
def test_near_million_duplicate_rows_rejected_within_256_mib():
    """The incident scenario, in a subprocess capped at 256 MiB (RLIMIT_AS)."""
    import resource  # noqa: F401  (POSIX only; guarded by skipif)

    child = textwrap.dedent(f"""
        import resource, sys
        LIMIT = 256 * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (LIMIT, LIMIT))
        sys.path.insert(0, {str(ROOT)!r})
        from app.validation import CsvValidationError, parse_and_validate

        # ~3.8 MiB payload: legal header + 1,000,000 duplicate hourly rows.
        raw = b"hour,pm25\\n" + b"5,1\\n" * 1_000_000
        assert len(raw) < 5 * 1024 * 1024  # within the existing upload cap
        try:
            parse_and_validate(raw)
        except CsvValidationError as exc:
            assert len(exc.reasons) == 100
            assert exc.error_counts == {{"duplicate hour": 999_999,
                                        "missing hour": 23}}
            assert exc.total_errors == 1_000_022
            assert exc.missing_hours == [h for h in range(24) if h != 5]
        else:
            raise AssertionError("expected CsvValidationError")
        peak_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        print(f"OK peak_rss={{peak_mib:.0f}} MiB")
    """)
    proc = subprocess.run(
        [sys.executable, "-I", "-c", child],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr
    assert "OK" in proc.stdout


# ---------------------------------------------------------------------------
# API level: clean 422, nothing published, service stays responsive
# ---------------------------------------------------------------------------

def test_huge_duplicate_upload_rejected_and_nothing_published(client, tmp_path):
    n = 300_000  # ~1.2 MB; floods the detail cap many times over
    raw = b"hour,pm25\n" + b"7,9\n" * n
    resp = _upload(client, "ST-001", "2026-11-14", raw)
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"] == "validation_failed"
    assert len(body["reasons"]) == 100
    assert body["details_truncated"] is True
    assert body["error_counts"] == {"duplicate hour": n - 1, "missing hour": 23}
    assert body["total_errors"] == n - 1 + 23
    assert body["missing_hours"] == [h for h in range(24) if h != 7]

    # The invalid batch produced no version, no records and no archived file.
    assert client.get("/stations/ST-001/dates/2026-11-14/records").status_code == 404
    versions = client.get("/stations/ST-001/dates/2026-11-14/versions").json()
    assert versions["count"] == 0
    assert list((tmp_path / "data" / "files").rglob("*.csv")) == []
    assert list((tmp_path / "data" / "tmp").glob("*")) == []


def test_small_invalid_file_response_shape_unchanged(client):
    rows = [(h, 2.5) for h in range(24) if h not in (3, 11)]
    rows = [(h, -0.1 if h == 7 else v) for h, v in rows]  # hour 7 also drops out
    raw = ("hour,pm25\n" + "\n".join(f"{h},{v}" for h, v in rows) + "\n").encode()
    resp = _upload(client, "ST-001", "2026-11-14", raw)
    assert resp.status_code == 422
    body = resp.json()
    assert body["details_truncated"] is False
    assert body["total_errors"] == len(body["reasons"])
    missing = {r["hour"] for r in body["reasons"] if r["reason"] == "missing hour"}
    assert {3, 7, 11} <= missing  # hour 7's row was rejected (negative value)
    assert body["missing_hours"] == [3, 7, 11]
    assert any("non-negative" in r["reason"] for r in body["reasons"])


def test_queries_stay_responsive_during_large_validation(client, monkeypatch):
    # Seed published data for station A.
    seeded = _upload(client, "ST-A", "2026-11-14", full_csv())
    assert seeded.status_code == 201

    # Track the exact window in which the CPU-bound validation is running.
    import app.main as main_module

    real_parse = main_module.parse_and_validate
    validating = threading.Event()

    def tracking_parse(raw):
        validating.set()
        try:
            return real_parse(raw)
        finally:
            validating.clear()

    monkeypatch.setattr(main_module, "parse_and_validate", tracking_parse)

    # Station B uploads the pathological file (~3.8 MB, 1M duplicate rows).
    raw = b"hour,pm25\n" + b"3,2\n" * 1_000_000

    def big_upload():
        return _upload(client, "ST-B", "2026-11-14", raw)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(big_upload)
        assert validating.wait(timeout=60), "validation never started"
        responsive_polls = 0
        while validating.is_set():
            resp = client.get("/stations/ST-A/dates/2026-11-14/records")
            assert resp.status_code == 200
            if validating.is_set():
                responsive_polls += 1
        upload_resp = future.result()

    # Other stations' published records stayed queryable *while the
    # validation of the huge file was actually running* (a validation
    # blocking the event loop would freeze every poll until it finished).
    assert responsive_polls >= 1
    assert upload_resp.status_code == 422

    # Station A's data is untouched; station B published nothing.
    body = client.get("/stations/ST-A/dates/2026-11-14/records").json()
    assert body["version"] == 1 and len(body["records"]) == 24
    assert client.get("/stations/ST-B/dates/2026-11-14/records").status_code == 404


def test_valid_24h_upload_still_works_after_flood_rejection(client):
    # Reject the flood first, then confirm normal upload/backfill rules.
    _upload(client, "ST-001", "2026-11-14", b"hour,pm25\n" + b"5,1\n" * 1000)

    ok = _upload(client, "ST-001", "2026-11-14", full_csv())
    assert ok.status_code == 201
    assert ok.json()["version"] == 1

    stale = _upload(client, "ST-001", "2026-11-14", full_csv(), expected_version=7)
    assert stale.status_code == 409
    assert stale.json()["current_version"] == 1

    backfill = _upload(client, "ST-001", "2026-11-14",
                       full_csv({h: 9.0 for h in range(24)}), expected_version=1)
    assert backfill.status_code == 201
    assert backfill.json()["version"] == 2
    current = client.get("/stations/ST-001/dates/2026-11-14/records").json()
    assert current["records"][0]["pm25"] == 9.0
