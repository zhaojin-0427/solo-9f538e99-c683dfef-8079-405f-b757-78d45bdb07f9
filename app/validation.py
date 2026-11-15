"""CSV parsing and batch validation.

A batch is only valid when *all* of the following hold:

* the header is exactly ``hour,pm25`` (a few common aliases are accepted);
* every row has exactly two fields;
* every hour is an integer in 0..23, every concentration is a non-negative
  finite number;
* hours 0..23 are all present exactly once (no duplicates, no gaps);
* the file is not empty apart from the header.

Any failure rejects the whole batch; the caller must never publish a batch
that contains an error.

Bounded-memory reporting
------------------------
Uploads are capped at 5 MiB, but a pathological file can still contain close
to a million offending rows. Validation is therefore a single-pass streaming
scan whose memory footprint does **not** depend on the row count:

* only the first ``MAX_DETAILED_REASONS`` (100) per-row error entries are
  kept verbatim; every error is still counted per reason string, so the
  response always carries exact totals for each error type;
* the complete list of missing hours (at most 24 entries) is always
  reported;
* field values and rows echoed back inside error entries are clipped, so a
  single multi-megabyte "cell" cannot accumulate either.

The whole validation of a within-limit file thus fits comfortably in a
256 MiB memory budget alongside the web worker itself.
"""
from __future__ import annotations

import csv
import hashlib
import io
import math
from collections import Counter
from dataclasses import dataclass, field

HOUR_ALIASES = {"hour", "小时", "时"}
PM25_ALIASES = {"pm25", "pm2.5", "concentration", "浓度", "pm2_5"}
REQUIRED_HOURS = set(range(24))

# Detailed per-row error entries kept verbatim in the response. Errors beyond
# this cap are still counted (per reason) but their details are dropped.
MAX_DETAILED_REASONS = 100
# Echoed field values / rows stored inside error entries are clipped so that
# one oversized cell cannot blow up the report.
MAX_VALUE_CHARS = 128
MAX_ROW_FIELDS = 8

# A whole upload is capped at 5 MiB by the API layer, so no legitimate single
# CSV field can exceed that. The default 128 KiB csv field limit would abort
# the scan of an otherwise within-limit file containing one oversized junk
# cell; raise it so such files are fully scanned and reported as row errors
# instead of crashing the parser.
csv.field_size_limit(256 * 1024 * 1024)


class CsvValidationError(Exception):
    """Raised when the uploaded CSV cannot be parsed or validated.

    Attributes
    ----------
    reasons:
        The first ``MAX_DETAILED_REASONS`` detailed error entries, in file
        order (identical to the historical behaviour for normal small files).
    error_counts:
        Exact total number of errors per reason string (never truncated).
    total_errors:
        Exact total number of errors across all reason types.
    missing_hours:
        Every missing hour in 0..23, sorted (never truncated; at most 24).
    """

    def __init__(
        self,
        reasons: list[dict[str, object]],
        *,
        error_counts: dict[str, int] | None = None,
        total_errors: int | None = None,
        missing_hours: list[int] | None = None,
    ):
        super().__init__("CSV validation failed")
        self.reasons = reasons
        self.error_counts = dict(error_counts) if error_counts else {}
        self.total_errors = len(reasons) if total_errors is None else total_errors
        self.missing_hours = list(missing_hours) if missing_hours else []

    @property
    def details_truncated(self) -> bool:
        """True when some per-row error details were dropped (counts kept)."""
        return self.total_errors > len(self.reasons)


@dataclass
class ParsedBatch:
    """A fully validated daily batch."""

    rows: list[tuple[int, float]]  # (hour, concentration), sorted by hour
    raw_bytes: bytes
    checksum: str
    line_count: int  # number of data lines (always 24 for a valid batch)


@dataclass
class _Collector:
    """Bounded error accumulation: exact totals, capped verbatim details."""

    reasons: list[dict[str, object]] = field(default_factory=list)
    counts: Counter = field(default_factory=Counter)
    total: int = 0

    def add(self, reason: str, line: int | None = None, **extra: object) -> None:
        self.total += 1
        self.counts[reason] += 1
        if len(self.reasons) >= MAX_DETAILED_REASONS:
            return  # keep counting; stop storing per-row details
        entry: dict[str, object] = {"reason": reason}
        if line is not None:
            entry["line"] = line
        entry.update(extra)
        self.reasons.append(entry)


def _reject(collector: _Collector, missing_hours: list[int] | None = None) -> None:
    raise CsvValidationError(
        collector.reasons,
        error_counts=dict(collector.counts),
        total_errors=collector.total,
        missing_hours=missing_hours or [],
    )


def _clip_text(text: str) -> str:
    """Bound the length of an echoed value stored in an error entry."""
    if len(text) <= MAX_VALUE_CHARS:
        return text
    return text[:MAX_VALUE_CHARS] + "…"


def _clip_row(row: list[str]) -> list[str]:
    """Bound the size of an echoed CSV row stored in an error entry."""
    clipped = [_clip_text(cell) for cell in row[:MAX_ROW_FIELDS]]
    if len(row) > MAX_ROW_FIELDS:
        clipped.append(f"… ({len(row) - MAX_ROW_FIELDS} more fields)")
    return clipped


def parse_and_validate(raw: bytes) -> ParsedBatch:
    """Parse raw CSV bytes and validate the whole batch.

    Raises :class:`CsvValidationError` with capped details plus exact
    per-type totals and the full missing-hour list on any problem.
    """
    collector = _Collector()

    # Decode tolerantly: utf-8-sig strips a possible BOM; replace undecodable
    # bytes rather than crashing, then report structural problems precisely.
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("utf-8", errors="replace")
        collector.add("file is not valid UTF-8 text")

    if not text.strip():
        collector.add("uploaded file is empty")
        _reject(collector)

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        collector.add("missing header row, expected 'hour,pm25'")
        _reject(collector)
    except csv.Error as exc:
        collector.add("CSV parse error while reading header",
                      detail=_clip_text(str(exc)))
        _reject(collector)

    # Check the column count before normalizing so a pathologically wide
    # header line never materializes a huge normalized list.
    if len(header) != 2:
        collector.add(
            "invalid header; expected two columns 'hour,pm25'",
            header=_clip_row(header),
        )
        _reject(collector)
    normalized_header = [header[0].strip().lower(), header[1].strip().lower()]
    if normalized_header[0] not in HOUR_ALIASES \
            or normalized_header[1] not in PM25_ALIASES:
        collector.add(
            "invalid header; expected two columns 'hour,pm25'",
            header=_clip_row(header),
        )
        _reject(collector)

    seen_hours: dict[int, int] = {}
    parsed_rows: list[tuple[int, float]] = []
    data_line = 1  # header is line 1; first data line is line 2

    try:
        for row in reader:
            data_line += 1

            # Skip completely blank lines but flag lines with stray whitespace
            # fields rather than silently dropping data.
            if len(row) == 0 or (len(row) == 1 and row[0].strip() == ""):
                continue

            if len(row) != 2:
                collector.add(
                    "expected exactly 2 columns (hour, pm25)",
                    line=data_line,
                    row=_clip_row(row),
                )
                continue

            hour_text, pm_text = row[0].strip(), row[1].strip()

            hour = _parse_hour(hour_text, data_line, collector)
            concentration = _parse_concentration(pm_text, data_line, collector)
            if hour is None or concentration is None:
                continue

            if hour in seen_hours:
                collector.add(
                    "duplicate hour",
                    line=data_line,
                    hour=hour,
                    first_line=seen_hours[hour],
                )
                continue

            seen_hours[hour] = data_line
            parsed_rows.append((hour, concentration))
    except csv.Error as exc:
        # Structural CSV failure (e.g. a field beyond the raised size limit):
        # report it as a batch error instead of crashing with a 500.
        collector.add("CSV parse error", line=data_line + 1,
                      detail=_clip_text(str(exc)))

    if not parsed_rows and not collector.reasons:
        collector.add("no data rows found")

    missing = sorted(REQUIRED_HOURS - set(seen_hours))
    if missing:
        # Missing hours are few (<= 24) and always reported in full via
        # ``missing_hours``; per-hour entries also flow through the same
        # capped detail list for small files.
        for hour in missing:
            collector.add("missing hour", hour=hour)

    if collector.total:
        _reject(collector, missing_hours=missing)

    parsed_rows.sort(key=lambda item: item[0])

    return ParsedBatch(
        rows=parsed_rows,
        raw_bytes=raw,
        checksum=hashlib.sha256(raw).hexdigest(),
        line_count=len(parsed_rows),
    )


def _parse_hour(text: str, line: int, collector: _Collector) -> int | None:
    if text == "":
        collector.add("empty hour value", line=line)
        return None
    try:
        # int() rejects floats like "8.0"; require whole-number syntax.
        hour = int(text)
    except ValueError:
        collector.add("hour is not an integer", line=line, value=_clip_text(text))
        return None
    if not 0 <= hour <= 23:
        collector.add(
            "hour out of range (must be 0..23)", line=line, hour=hour
        )
        return None
    return hour


def _parse_concentration(text: str, line: int, collector: _Collector) -> float | None:
    if text == "":
        collector.add("empty pm25 value", line=line)
        return None
    try:
        value = float(text)
    except ValueError:
        collector.add("pm25 concentration is not a number", line=line,
                      value=_clip_text(text))
        return None
    if math.isnan(value) or math.isinf(value):
        collector.add(
            "pm25 concentration must be a finite number", line=line,
            value=_clip_text(text),
        )
        return None
    if value < 0:
        collector.add(
            "pm25 concentration must be non-negative", line=line, value=value
        )
        return None
    return value
