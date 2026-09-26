"""
Unit tests for the batch data-quality gate.

A gate that never fails is decoration. Each check is exercised with a
deliberately broken frame and asserted to fail, plus one clean frame asserted
to pass.
"""
import os
import sys
from datetime import datetime, timezone

import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))
os.environ.setdefault("ES_HOST", "http://localhost:9201")

from validation import validate_customer_dimension  # noqa: E402

CLEAN = pd.DataFrame({
    "customer_id": [1, 2, 3],
    "name": ["Aarav Sharma", "Priya Nair", "Rohan Mehta"],
    "city": ["Hyderabad", "Chennai", "Bangalore"],
    "segment": ["Premium", "Standard", "Premium"],
    "signup_date": pd.to_datetime(["2023-01-15", "2023-03-22", "2022-11-05"]),
    "updated_at": [datetime.now(timezone.utc).replace(tzinfo=None)] * 3,
})


def check(name, df, expect_fail, **kwargs):
    report = validate_customer_dimension(df, **kwargs)
    failed = {r.check for r in report.failures}
    if expect_fail:
        assert name in failed, f"{name}: expected FAIL, got failures={failed or 'none'}"
        print(f"  PASS  {name:42s} -> blocked")
    else:
        assert report.ok, f"{name}: expected clean, failures={failed}"
        print(f"  PASS  {name:42s} -> clean ({len(report.results)} checks)")
    return report


print("baseline: a clean frame passes every check")
check("clean_frame", CLEAN.copy(), expect_fail=False, expected_rows=3)

print("\neach check fires on the data it is meant to catch")

d = CLEAN.copy(); d.loc[0, "name"] = None
check("null_rate", d, expect_fail=True)

d = CLEAN.copy(); d.loc[1, "name"] = "   "
check("no_empty_strings", d, expect_fail=True)

d = CLEAN.copy(); d.loc[2, "customer_id"] = 1
check("customer_id_unique", d, expect_fail=True)

d = CLEAN.copy(); d["customer_id"] = ["1", "abc", "3"]
check("customer_id_numeric", d, expect_fail=True)

d = CLEAN.copy(); d["customer_id"] = [1, -2, 3]
check("customer_id_positive", d, expect_fail=True)

d = CLEAN.copy(); d.loc[0, "segment"] = "Platinum"
check("segment_domain", d, expect_fail=True)

d = CLEAN.drop(columns=["city"])
check("required_columns_present", d, expect_fail=True)

d = CLEAN.iloc[0:0]
check("row_count_minimum", d, expect_fail=True)

d = CLEAN.copy(); d["signup_date"] = ["not-a-date", "2023-01-01", "2023-01-01"]
check("signup_date_parseable", d, expect_fail=True)

print("\nreconciliation and freshness")

d = CLEAN.copy()
d["updated_at"] = datetime.now(timezone.utc).replace(tzinfo=None) - pd.Timedelta(days=3)
check("snapshot_freshness", d, expect_fail=True, expected_rows=3)

check("row_count_reconciles_with_source", CLEAN.copy(), expect_fail=True, expected_rows=99)

future = datetime.now(timezone.utc).replace(tzinfo=None) + pd.Timedelta(hours=2)
check("no_rows_lost_against_source_watermark", CLEAN.copy(), expect_fail=True,
      source_max_updated_at=future)

print("\nfuture signup dates warn but do not block publication")
rep = validate_customer_dimension(
    CLEAN.assign(signup_date=pd.to_datetime(["2030-01-01", "2023-03-22", "2022-11-05"])),
    expected_rows=3,
)
warned = {r.check for r in rep.warnings}
assert "signup_date_not_future" in warned, warned
assert rep.ok, "a future signup date must not block publication"
print(f"  PASS  {'warns without blocking':42s} -> warnings={sorted(warned)}")

print("\nALL VALIDATION TESTS PASSED")
