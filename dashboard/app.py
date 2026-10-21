"""
Live operations dashboard for the UPI three-way reconciliation demo.

Design notes:
  * Every query is static SQL; connection parameters come from the environment.
  * The live block is an ``st.fragment`` so auto-refresh does not rerun the whole
    page (and does not create a sleep/rerun loop that fights Streamlit's script
    lifecycle).
  * Finance control totals are shown next to detection quality.  A pipeline that
    misses breaks is not "accurate"; a pipeline that invents them is not "safe".
  * The same app runs in three places: the Docker stack (env var), Streamlit
    Community Cloud (``st.secrets``) and anywhere else with a public Postgres.
"""
from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(
    page_title="UPI Reconciliation",
    page_icon="₹",
    layout="wide",
)


def _dsn() -> str:
    """
    Resolve the Postgres DSN.

    Streamlit Community Cloud injects secrets through ``st.secrets``; the Docker
    stack uses the environment.  Both are supported so the dashboard can be
    published without touching the code.
    """
    try:
        secret = st.secrets.get("POSTGRES_DSN")
        if secret:
            return str(secret)
    except Exception:  # noqa: BLE001 - no secrets configured (local run)
        pass
    return os.environ.get(
        "POSTGRES_DSN",
        "postgresql://app:app@postgres:5432/fintech",
    )


DSN = _dsn()
TIME_SCALE = float(os.environ.get("TIME_SCALE", "60"))


@st.cache_resource
def get_conn():
    conn = psycopg2.connect(DSN, connect_timeout=5)
    conn.autocommit = True
    return conn


def q(sql: str) -> pd.DataFrame:
    try:
        return pd.read_sql(sql, get_conn())
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        # Postgres restarts are common during a demo; drop the cached handle and
        # retry once rather than rendering a permanently broken page.
        get_conn.clear()
        return pd.read_sql(sql, get_conn())


def safe_q(sql: str) -> pd.DataFrame:
    try:
        return q(sql)
    except psycopg2.OperationalError as exc:
        # The database is not reachable yet (the pipeline is still starting, or
        # the hosted Postgres is not exposed to this app).  Say so instead of
        # dumping a driver error at the reader.
        st.warning(
            "Cannot reach the reconciliation database. It must be reachable from "
            f"where this app runs. ({exc.__class__.__name__}: {str(exc).splitlines()[0]})"
        )
        return pd.DataFrame()
    except Exception as exc:  # tables may not exist for the first few seconds
        st.warning(f"Query failed: {exc}")
        return pd.DataFrame()


def inr(value) -> str:
    try:
        return f"₹{float(value):,.2f}"
    except (TypeError, ValueError):
        return "₹0.00"


st.title("UPI three-way reconciliation")
st.caption(
    "Switch ⟕ merchant callback ⟕ bank settlement · amounts in paise internally · "
    f"demo time scale {TIME_SCALE:g}× (4 h SLA ≈ {4 * 60 / TIME_SCALE:.0f} simulated minutes)"
)


@st.fragment(run_every="5s")
def live() -> None:
    k = safe_q(
        """
        select count(*) as total,
               coalesce(sum(case when match_status = 'MATCHED' then 1 else 0 end), 0) as matched,
               coalesce(sum(case when match_status <> 'MATCHED' then 1 else 0 end), 0) as unmatched,
               percentile_cont(0.99) within group (order by settlement_lag_s) as p99,
               max(decided_at) as latest
        from recon.match_results
        where decided_at > now() - interval '15 minutes'
        """
    )
    if k.empty:
        st.info("Waiting for the first decisions …")
        return
    row = k.iloc[0]
    total = int(row["total"] or 0)
    matched = int(row["matched"] or 0)
    unmatched = int(row["unmatched"] or 0)

    tq = safe_q(
        """
        select count(*) as injected,
               coalesce(sum(case when m.match_status = g.expected_status then 1 else 0 end), 0) as final_ok,
               coalesce(sum(case when m.match_status = g.expected_initial then 1 else 0 end), 0) as initial_ok
        from recon.ground_truth g
        left join recon.match_results m using (rrn)
        where g.injected_at > now() - interval '2 hours'
        """
    )
    injected = int(tq.iloc[0]["injected"] or 0) if not tq.empty else 0
    final_ok = int(tq.iloc[0]["final_ok"] or 0) if not tq.empty else 0
    initial_ok = int(tq.iloc[0]["initial_ok"] or 0) if not tq.empty else 0

    lc = safe_q(
        """
        select count(*) as corrected
        from (
          select rrn
          from recon.decision_audit
          where decided_at > now() - interval '2 hours'
          group by rrn
          having bool_or(match_status = 'MISSING_SETTLEMENT')
             and bool_or(match_status = 'MATCHED')
        ) x
        """
    )
    corrected = int(lc.iloc[0]["corrected"] or 0) if not lc.empty else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Decisions (15m)", f"{total:,}")
    c2.metric("Match rate", f"{100 * matched / max(total, 1):.2f}%")
    c3.metric("Unmatched", f"{unmatched:,}")
    c4.metric("Settlement lag p99", f"{(row['p99'] or 0) / 60:.1f} sim min")
    c5.metric("Injected faults (2h)", f"{injected:,}", help="Samples in recon.ground_truth")
    c6.metric(
        "Final recall",
        f"{100 * final_ok / injected:.1f}%" if injected else "—",
        help=f"Initial-status recall {100 * initial_ok / injected:.1f}% · late corrections {corrected:,}"
        if injected
        else "No ground-truth samples yet",
    )

    # ---- finance control totals -------------------------------------------------
    totals = safe_q(
        """
        select coalesce(sum(switch_amount), 0) as gross,
               coalesce(sum(case when match_status = 'MATCHED' then switch_amount end), 0) as reconciled,
               coalesce(sum(case when match_status = 'AMOUNT_MISMATCH' then switch_amount end), 0) as mismatch,
               coalesce(sum(case when match_status = 'MISSING_SETTLEMENT' then switch_amount end), 0) as outstanding,
               coalesce(sum(case when match_status = 'MISSING_CALLBACK' then switch_amount end), 0) as missing_callback
        from recon.match_results
        where decided_at > now() - interval '1 hour'
        """
    )
    st.subheader("Finance control totals — last hour (simulated timestamps)")
    if not totals.empty:
        t = totals.iloc[0]
        f1, f2, f3, f4, f5 = st.columns(5)
        f1.metric("Gross", inr(t["gross"]))
        f2.metric("Reconciled", inr(t["reconciled"]))
        f3.metric("Amount mismatch (full value)", inr(t["mismatch"]))
        f4.metric("Outstanding settlement", inr(t["outstanding"]))
        f5.metric("Missing callback", inr(t["missing_callback"]))

    left, right = st.columns(2)

    ts = safe_q(
        """
        select date_trunc('minute', decided_at) as minute,
               match_status,
               count(*) as decisions
        from recon.decision_audit
        where decided_at > now() - interval '60 minutes'
        group by 1, 2
        order by 1
        """
    )
    if ts.empty:
        left.info("No decision history yet.")
    else:
        left.plotly_chart(
            px.area(
                ts,
                x="minute",
                y="decisions",
                color="match_status",
                title="Decision throughput by status (per minute)",
            ),
            use_container_width=True,
        )

    merchants = safe_q(
        """
        select merchant_id,
               count(*) as txns,
               round(100.0 * sum(case when match_status <> 'MATCHED' then 1 else 0 end) / count(*), 2) as unmatched_pct,
               max(decided_at) as latest
        from recon.match_results
        where decided_at > now() - interval '15 minutes'
        group by 1
        having count(*) >= 20
        order by unmatched_pct desc
        limit 10
        """
    )
    right.subheader("Merchant health (last 15m, ≥20 txns)")
    right.dataframe(merchants, use_container_width=True, hide_index=True)

    a, b = st.columns(2)
    alerts = safe_q(
        """
        select merchant_id, window_start, unmatched_rate, message, created_at
        from recon.alerts
        order by created_at desc
        limit 20
        """
    )
    a.subheader("Alerts — unmatched rate > 10%")
    a.dataframe(alerts, use_container_width=True, hide_index=True)

    quality = safe_q(
        """
        select g.fault,
               count(*) as injected,
               round(100.0 * sum(case when m.match_status = g.expected_initial then 1 else 0 end) / count(*), 1) as initial_recall_pct,
               round(100.0 * sum(case when m.match_status = g.expected_status then 1 else 0 end) / count(*), 1) as final_recall_pct,
               sum(case when g.fault = 'LATE_SETTLE' and m.settle_amount is not null then 1 else 0 end) as late_corrected
        from recon.ground_truth g
        left join recon.match_results m using (rrn)
        where g.injected_at > now() - interval '2 hours'
        group by 1
        order by 1
        """
    )
    b.subheader("Detection quality by injected fault")
    b.dataframe(quality, use_container_width=True, hide_index=True)

    st.subheader("Exception queue (latest 200 current-state exceptions)")
    exceptions = safe_q(
        """
        select rrn, merchant_id, match_status,
               switch_amount, settle_amount, callback_amount,
               switch_ts,
               round(extract(epoch from (now() - switch_ts)) / 60.0, 1) as age_min
        from recon.match_results
        where match_status <> 'MATCHED'
        order by decided_at desc
        limit 200
        """
    )
    st.dataframe(exceptions, use_container_width=True, hide_index=True)

    if row["latest"] is not None:
        st.caption(f"Last decision written at {row['latest']} · auto-refresh every 5 s")


live()
