"""
Data quality validation for the batch-extracted customer dimension.

A file-exists check tells you a job ran. These checks tell you whether the
data is fit to publish, which is the question that actually matters: a
pipeline that silently writes wrong numbers is worse than one that fails.

Every check returns a ValidationResult so the orchestrator can report all
failures at once instead of dying on the first one. Thresholds are
configurable so the same rules can be tightened as the data matures.

Check families:
  completeness  rows present, columns present, null rates
  uniqueness    primary key integrity
  validity      types, ranges, allowed categorical domains
  consistency   reconciliation against the source row count
  freshness     how old the snapshot is
"""
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

sys.path.append(os.path.dirname(__file__))
from utils.es_logger import get_logger

logger = get_logger("validation")

MIN_ROW_COUNT = int(os.getenv("DQ_MIN_ROWS", "1"))
MAX_NULL_RATE = float(os.getenv("DQ_MAX_NULL_RATE", "0.0"))
MAX_AGE_HOURS = float(os.getenv("DQ_MAX_AGE_HOURS", "26"))
REQUIRED_COLUMNS = ("customer_id", "name", "city", "segment", "signup_date")
ALLOWED_SEGMENTS = {"Premium", "Standard"}


@dataclass
class ValidationResult:
    check: str
    passed: bool
    detail: str
    severity: str = "error"

    def __str__(self):
        mark = "PASS" if self.passed else ("WARN" if self.severity == "warning" else "FAIL")
        return f"[{mark}] {self.check}: {self.detail}"


@dataclass
class ValidationReport:
    results: list = field(default_factory=list)

    def add(self, check, passed, detail, severity="error"):
        self.results.append(ValidationResult(check, passed, detail, severity))
        return self

    @property
    def failures(self):
        return [r for r in self.results if not r.passed and r.severity == "error"]

    @property
    def warnings(self):
        return [r for r in self.results if not r.passed and r.severity == "warning"]

    @property
    def ok(self):
        return not self.failures

    def emit(self):
        for r in self.results:
            (logger.info if r.passed else (logger.warning if r.severity == "warning" else logger.error))(str(r))
        logger.info(
            f"validation summary: {len(self.results) - len(self.failures) - len(self.warnings)} passed, "
            f"{len(self.warnings)} warnings, {len(self.failures)} failures"
        )

    def raise_if_failed(self):
        if not self.ok:
            failed = ", ".join(r.check for r in self.failures)
            raise ValueError(f"data quality gate failed: {failed}")


def validate_customer_dimension(
    df: pd.DataFrame,
    expected_rows: int = None,
    source_max_updated_at: datetime = None,
) -> ValidationReport:
    report = ValidationReport()

    # ---- completeness -------------------------------------------------
    report.add(
        "row_count_minimum",
        len(df) >= MIN_ROW_COUNT,
        f"{len(df)} rows (minimum {MIN_ROW_COUNT})",
    )

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    report.add(
        "required_columns_present",
        not missing,
        "all present" if not missing else f"missing {missing}",
    )

    if not missing:
        null_rates = {c: float(df[c].isna().mean()) for c in REQUIRED_COLUMNS}
        worst = max(null_rates.items(), key=lambda kv: kv[1])
        report.add(
            "null_rate",
            worst[1] <= MAX_NULL_RATE,
            f"worst column {worst[0]} at {worst[1]:.1%} (max {MAX_NULL_RATE:.1%}); "
            f"per-column {{{', '.join(f'{c}: {v:.0%}' for c, v in null_rates.items())}}}",
        )

        blanks = {
            c: int(df[c].astype(str).str.strip().eq("").sum())
            for c in ("name", "city", "segment")
        }
        report.add(
            "no_empty_strings",
            sum(blanks.values()) == 0,
            f"blanks per column {blanks}",
        )

    # ---- uniqueness ----------------------------------------------------
    if "customer_id" in df.columns:
        dupes = int(df["customer_id"].duplicated().sum())
        report.add(
            "customer_id_unique",
            dupes == 0,
            f"{dupes} duplicate customer_id values",
        )
        non_numeric = int(pd.to_numeric(df["customer_id"], errors="coerce").isna().sum())
        report.add(
            "customer_id_numeric",
            non_numeric == 0,
            f"{non_numeric} non-numeric customer_id values",
        )
        non_positive = int((pd.to_numeric(df["customer_id"], errors="coerce") <= 0).sum())
        report.add(
            "customer_id_positive",
            non_positive == 0,
            f"{non_positive} customer_id values <= 0",
        )

    # ---- validity ------------------------------------------------------
    if "segment" in df.columns:
        unexpected = sorted(set(df["segment"].dropna().unique()) - ALLOWED_SEGMENTS)
        report.add(
            "segment_domain",
            not unexpected,
            "all within {Premium, Standard}" if not unexpected
            else f"unexpected segments {unexpected} (this would break gold marts)",
        )

    if "signup_date" in df.columns:
        parsed = pd.to_datetime(df["signup_date"], errors="coerce")
        report.add("signup_date_parseable", int(parsed.isna().sum()) == 0,
                   f"{int(parsed.isna().sum())} unparseable signup_date values")
        future = int((parsed > pd.Timestamp.now(tz=None)).sum())
        report.add("signup_date_not_future", future == 0,
                   f"{future} signup dates in the future", severity="warning")

    # ---- consistency ---------------------------------------------------
    if expected_rows is not None:
        delta = len(df) - expected_rows
        report.add(
            "row_count_reconciles_with_source",
            delta == 0,
            f"lake has {len(df)}, source had {expected_rows}, delta {delta}",
        )

    # ---- freshness -----------------------------------------------------
    if "updated_at" in df.columns and len(df):
        newest = pd.to_datetime(df["updated_at"], errors="coerce", utc=True).max()
        if pd.notna(newest):
            age_hours = (datetime.now(timezone.utc) - newest.to_pydatetime()).total_seconds() / 3600
            report.add(
                "snapshot_freshness",
                age_hours <= MAX_AGE_HOURS,
                f"newest record {age_hours:.2f}h old (max {MAX_AGE_HOURS}h)",
            )
        else:
            report.add("snapshot_freshness", False, "updated_at could not be parsed", severity="warning")

    if source_max_updated_at is not None and "updated_at" in df.columns and len(df):
        lake_max = pd.to_datetime(df["updated_at"], errors="coerce", utc=True).max()
        source_max = pd.Timestamp(source_max_updated_at)
        if source_max.tzinfo is None:
            source_max = source_max.tz_localize("UTC")
        lag = (lake_max - source_max).total_seconds()
        report.add(
            "no_rows_lost_against_source_watermark",
            lag >= 0,
            f"lake max(updated_at) is {lag:.0f}s ahead of the source watermark"
            if lag >= 0 else f"lake max(updated_at) is {-lag:.0f}s BEHIND the source watermark",
        )

    return report
