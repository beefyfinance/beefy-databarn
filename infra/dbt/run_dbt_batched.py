#!/usr/bin/env python3
"""Run `make dbt reimport` / `make dbt refresh`, batching the models that ask for it.

Usage (from the dbt project dir, inside the uv env):
  run_dbt_batched.py reimport <selector> <since>
  run_dbt_batched.py refresh  <selector>

A model opts in with config meta (e.g. product_stats.yml):
  meta:
    batch: month            # one dbt run per calendar month
    batch_start: "2021-07-31"  # where a full refresh starts

and must honour the incremental_since / incremental_until vars (macros/incremental_since.sql),
also when not incremental (first batch of a full refresh).

Plan for a selection that contains batchable models B:
  1. everything selected that is not B or downstream of B, once
  2. each B, one run per month from <since> (or batch_start) to tomorrow;
     for refresh the first batch is a --full-refresh, later batches append to it
  3. everything selected downstream of B, once
Without batchable models in the selection it is a single plain dbt run, as before.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, timedelta


def dbt(*args: str) -> None:
    cmd = ["dbt", *args]
    print("+ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def batchable_models(selector: str) -> list[tuple[str, str]]:
    """(name, batch_start) of selected models whose meta asks for monthly batches."""
    out = subprocess.run(
        ["dbt", "--quiet", "ls", "--resource-type", "model", "--select", selector, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    found = []
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        node = json.loads(line)
        meta = (node.get("config") or {}).get("meta") or {}
        if meta.get("batch") == "month":
            found.append((node["name"], str(meta.get("batch_start", ""))))
    return found


def next_month(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def month_windows(start: date) -> list[tuple[date, date]]:
    end = date.today() + timedelta(days=1)
    windows = []
    current = start
    while current < end:
        upper = next_month(current)
        windows.append((current, upper))
        current = upper
    return windows


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[1] not in ("reimport", "refresh"):
        sys.exit(__doc__)
    mode, selector = sys.argv[1], sys.argv[2]
    since = sys.argv[3] if len(sys.argv) > 3 else None
    if mode == "reimport" and not since:
        sys.exit("reimport needs <since> (YYYY-MM-DD)")

    full_refresh = ["--full-refresh"] if mode == "refresh" else []
    since_vars = ["--vars", json.dumps({"incremental_since": since})] if since else []

    batched = batchable_models(selector)
    if not batched:
        dbt("run", "--select", selector, *full_refresh, *since_vars)
        return

    names = [name for name, _ in batched]
    print(f"Batching by month: {', '.join(names)}", flush=True)
    downstream_of_batched = [f"{name}+" for name in names]

    # 1. Upstream and unrelated models, once.
    dbt("run", "--select", selector, "--exclude", *downstream_of_batched, *full_refresh, *since_vars)

    # 2. Batchable models, one month at a time.
    for name, batch_start in batched:
        start_str = since or batch_start
        if not start_str:
            sys.exit(f"{name}: meta.batch_start is required for refresh")
        for i, (lower, upper) in enumerate(month_windows(date.fromisoformat(start_str[:10]))):
            batch_vars = {"incremental_since": lower.isoformat(), "incremental_until": upper.isoformat()}
            print(f"=== {name}: {lower} -> {upper} ===", flush=True)
            first_refresh_batch = full_refresh if i == 0 else []
            dbt("run", "--select", name, *first_refresh_batch, "--vars", json.dumps(batch_vars))

    # 3. Selected models downstream of the batched ones, once.
    downstream = [f"{token},{name}+" for token in selector.split() for name in names]
    dbt("run", "--select", *downstream, "--exclude", *names, *full_refresh, *since_vars)


if __name__ == "__main__":
    main()
