#!/usr/bin/env python3
"""
Compute the run metrics of a simulation and write them as JSON.

The GitHub Actions workflow runs the whole stack on a runner, lets it produce
traffic, and then calls this script to turn the database into a small JSON
document.  The document is published to the ``runs`` branch and read by the
Vercel dashboard, so the simulation can be shown live without any long-lived
server.

Usage:
    python3 scripts/snapshot_metrics.py --dsn "$POSTGRES_DSN" --out runs/latest.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:  # pragma: no cover - the workflow installs it
    print("psycopg2 is required", file=sys.stderr)
    raise SystemExit(2)

STATUSES = ("MATCHED", "AMOUNT_MISMATCH", "MISSING_SETTLEMENT", "MISSING_CALLBACK")
FAULTS = (
    "AMOUNT_DRIFT",
    "DUP_SWITCH",
    "LATE_SETTLE",
    "MISSING_CALLBACK",
    "MISSING_SETTLE",
    "OUT_OF_ORDER",
)


def connect(dsn: str):
    conn = psycopg2.connect(dsn, connect_timeout=10)
    conn.autocommit = True
    return conn


def one(cur, sql, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def rows(cur, sql, params=()):
    cur.execute(sql, params)
    return [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]


def build(dsn: str, age_minutes: int) -> dict:
    conn = connect(dsn)
    try:
        with conn.cursor() as cur:
            total = one(cur, "select count(*) from recon.match_results")
            by_status = {
                r["match_status"]: r["n"]
                for r in rows(
                    cur,
                    "select match_status, count(*) as n from recon.match_results group by 1",
                )
            }
            audit_rows = one(cur, "select count(*) from recon.decision_audit")
            audit_rrns = one(cur, "select count(distinct rrn) from recon.decision_audit")
            corrections = one(
                cur,
                """
                select count(*) from (
                  select rrn from recon.decision_audit group by rrn
                  having bool_or(match_status='MISSING_SETTLEMENT')
                     and bool_or(match_status='MATCHED')
                ) x
                """,
            )
            regressions = one(
                cur,
                """
                select count(*) from (
                  select rrn,
                         (array_agg(match_status order by decision_ts asc))[1] as first_status,
                         (array_agg(match_status order by decision_ts desc))[1] as last_status
                  from recon.decision_audit group by rrn
                ) x
                where first_status = 'MATCHED' and last_status = 'MISSING_SETTLEMENT'
                """,
            )
            false_positives = one(
                cur,
                """
                select count(*) from recon.match_results m
                left join recon.ground_truth g using (rrn)
                where m.match_status <> 'MATCHED' and g.rrn is null
                """,
            )
            cur.execute(
                """
                select coalesce(sum(switch_amount), 0),
                       coalesce(sum(case when match_status='MATCHED' then switch_amount end), 0),
                       coalesce(sum(case when match_status='AMOUNT_MISMATCH' then switch_amount end), 0),
                       coalesce(sum(case when match_status='MISSING_SETTLEMENT' then switch_amount end), 0),
                       coalesce(sum(case when match_status='MISSING_CALLBACK' then switch_amount end), 0)
                from recon.match_results
                """
            )
            totals = cur.fetchone()
            alerts = rows(
                cur,
                """
                select merchant_id, window_start, unmatched_rate, message
                from recon.alerts order by created_at desc limit 10
                """,
            )
            merchants = rows(
                cur,
                """
                select merchant_id,
                       count(*) as txns,
                       round(100.0 * sum(case when match_status <> 'MATCHED' then 1 else 0 end)
                             / count(*), 2) as unmatched_pct
                from recon.match_results
                where decided_at > now() - interval '15 minutes'
                group by 1 having count(*) >= 20
                order by unmatched_pct desc limit 10
                """,
            )
            recall_sql = """
                select g.fault,
                       count(*) as samples,
                       count(*) filter (where m.match_status = g.expected_status) as final_ok,
                       count(*) filter (where m.match_status = g.expected_initial) as initial_ok
                from recon.ground_truth g
                left join recon.match_results m using (rrn)
                where g.injected_at < now() - make_interval(mins => %s)
                group by 1 order by 1
            """
            recall = rows(cur, recall_sql, (age_minutes,))
            aged_cohort = sum(r["samples"] for r in recall)
            if not aged_cohort:
                # The run was too short for the aged cohort; fall back to every
                # sample and say so, rather than reporting a misleading 0 %.
                recall = rows(
                    cur,
                    recall_sql.replace(
                        "where g.injected_at < now() - make_interval(mins => %s)",
                        "where true",
                    ),
                    (),
                )
                cohort_note = (
                    f"the run was shorter than the {age_minutes} minute window, "
                    "so every sample is counted"
                )
            else:
                cohort_note = f"samples older than {age_minutes} minutes"
            latency = rows(
                cur,
                """
                select round(avg(extract(epoch from (decided_at - switch_ts)))) as avg_s,
                       round(percentile_cont(0.5) within group
                             (order by extract(epoch from (decided_at - switch_ts)))) as p50_s,
                       round(percentile_cont(0.95) within group
                             (order by extract(epoch from (decided_at - switch_ts)))) as p95_s,
                       count(*) as n
                from recon.match_results
                where decided_at > now() - interval '30 minutes'
                """,
            )
    finally:
        conn.close()

    gross, reconciled, mismatch, outstanding, missing_cb = (float(x) for x in totals)
    cohort_note = cohort_note
    matched = by_status.get("MATCHED", 0)
    samples = sum(r["samples"] for r in recall)
    final_ok = sum(r["final_ok"] for r in recall)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "age_minutes": age_minutes,
        "cohort": cohort_note,
        "decisions": total,
        "by_status": by_status,
        "match_rate_pct": round(100.0 * matched / total, 2) if total else 0.0,
        "audit_rows": audit_rows,
        "audit_rrns": audit_rrns,
        "late_corrections": corrections,
        "regressions": regressions,
        "false_positives": false_positives,
        "control_total": {
            "gross_inr": round(gross, 2),
            "reconciled_inr": round(reconciled, 2),
            "mismatch_inr": round(mismatch, 2),
            "outstanding_inr": round(outstanding, 2),
            "missing_callback_inr": round(missing_cb, 2),
            "difference_inr": round(
                gross - reconciled - mismatch - outstanding - missing_cb, 2
            ),
        },
        "recall": [
            {
                "fault": r["fault"],
                "samples": r["samples"],
                "final_ok": r["final_ok"],
                "initial_ok": r["initial_ok"],
                "final_recall_pct": round(100.0 * r["final_ok"] / r["samples"], 2)
                if r["samples"]
                else 0.0,
            }
            for r in recall
        ],
        "recall_overall_pct": round(100.0 * final_ok / samples, 2) if samples else 0.0,
        "latency": latency[0] if latency else {},
        "alerts": alerts,
        "merchants": merchants,
        "statuses": list(STATUSES),
        "faults": list(FAULTS),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("POSTGRES_DSN", ""))
    parser.add_argument("--out", default="runs/latest.json")
    parser.add_argument("--history", default="runs/history.json")
    parser.add_argument("--age-minutes", type=int, default=25)
    args = parser.parse_args()

    if not args.dsn:
        print("no --dsn and no POSTGRES_DSN", file=sys.stderr)
        return 2

    doc = build(args.dsn, args.age_minutes)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, default=str)
    print(f"wrote {args.out}: {doc['decisions']} decisions, "
          f"recall {doc['recall_overall_pct']}%, "
          f"{doc['late_corrections']} late corrections")

    # A rolling history of the last 50 runs, so the dashboard can show a trend.
    history = []
    if os.path.exists(args.history):
        try:
            with open(args.history, encoding="utf-8") as fh:
                history = json.load(fh)
        except (ValueError, OSError):
            history = []
    history.append(
        {
            "generated_at": doc["generated_at"],
            "decisions": doc["decisions"],
            "match_rate_pct": doc["match_rate_pct"],
            "recall_overall_pct": doc["recall_overall_pct"],
            "late_corrections": doc["late_corrections"],
            "false_positives": doc["false_positives"],
            "regressions": doc["regressions"],
        }
    )
    with open(args.history, "w", encoding="utf-8") as fh:
        json.dump(history[-50:], fh, indent=2, default=str)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
