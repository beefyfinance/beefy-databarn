#!/usr/bin/env python3
"""Run `make dbt reimport` / `make dbt refresh`, batching the models that ask for it.

Usage (from the dbt project dir, inside the uv env):
  run_dbt_batched.py reimport <selector> <since>
  run_dbt_batched.py refresh  <selector>

A model opts in with config meta (e.g. fact_product_stats_1h.yml):
  meta:
    batch: month            # one dbt run per calendar month
    batch_start: "2021-07-31"  # where a full refresh starts

and must honour the incremental_since / incremental_until vars (macros/incremental_since.sql),
also when not incremental (first batch of a full refresh).

A batched model can sit downstream of another one, so the selection runs in levels, parents first.
The level of a model is the number of batched models above it in the selection. For each level:
  1. its models that are not batched, once
  2. each of its batched models, one run per month from <since> (or batch_start) to tomorrow;
     for refresh the first batch is a --full-refresh, later batches append to it
Without batchable models in the selection it is a single plain dbt run, as before.
After a reimport, macros/purge_deleted_rows.sql drops the rows its deletes hid (they stay on disk otherwise).
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, timedelta


def dbt(*args: str) -> None:
    cmd = ["dbt", *args]
    print("+ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


@dataclass
class Model:
    name: str
    parents: list[str]  # unique ids
    batch_start: str | None  # None when not batched


def selected_models(selector: str) -> dict[str, Model]:
    """Selected models by unique id, with the meta that asks for monthly batches."""
    out = subprocess.run(
        [
            "dbt", "--quiet", "ls", "--resource-type", "model", "--select", selector,
            "--output", "json", "--output-keys", "unique_id", "name", "config", "depends_on",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    found = {}
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        node = json.loads(line)
        meta = (node.get("config") or {}).get("meta") or {}
        batch_start = str(meta.get("batch_start", "")) if meta.get("batch") == "month" else None
        found[node["unique_id"]] = Model(node["name"], node["depends_on"]["nodes"], batch_start)
    return found


def levels(models: dict[str, Model]) -> dict[str, int]:
    """Level of each model: how many batched models it sits below, along its longest path."""
    found: dict[str, int] = {}

    def level(unique_id: str) -> int:
        if unique_id not in found:
            parents = [p for p in models[unique_id].parents if p in models]
            found[unique_id] = max(
                (level(p) + (models[p].batch_start is not None) for p in parents),
                default=0,
            )
        return found[unique_id]

    for unique_id in models:
        level(unique_id)
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

    models = selected_models(selector)
    if all(model.batch_start is None for model in models.values()):
        dbt("run", "--select", selector, *full_refresh, *since_vars)
    else:
        run_batched(models, since, full_refresh, since_vars)
    if mode == "reimport":
        purge_deleted_rows(models)


def purge_deleted_rows(models: dict[str, Model]) -> None:
    """A reimport hides the whole old history behind lightweight deletes: drop those rows from disk."""
    names = sorted(model.name for model in models.values())
    dbt("run-operation", "purge_deleted_rows", "--args", json.dumps({"models": names}))


def run_batched(models: dict[str, Model], since: str | None, full_refresh: list[str], since_vars: list[str]) -> None:
    names = sorted(model.name for model in models.values() if model.batch_start is not None)
    print(f"Batching by month: {', '.join(names)}", flush=True)

    model_levels = levels(models)
    for current_level in range(max(model_levels.values()) + 1):
        at_level = [model for uid, model in models.items() if model_levels[uid] == current_level]

        # 1. Models of this level that are not batched, once.
        plain = sorted(model.name for model in at_level if model.batch_start is None)
        if plain:
            dbt("run", "--select", *plain, *full_refresh, *since_vars)

        # 2. Batchable models, one month at a time.
        for model in sorted((m for m in at_level if m.batch_start is not None), key=lambda m: m.name):
            start_str = since or model.batch_start
            if not start_str:
                sys.exit(f"{model.name}: meta.batch_start is required for refresh")
            for i, (lower, upper) in enumerate(month_windows(date.fromisoformat(start_str[:10]))):
                batch_vars = {"incremental_since": lower.isoformat(), "incremental_until": upper.isoformat()}
                print(f"=== {model.name}: {lower} -> {upper} ===", flush=True)
                first_refresh_batch = full_refresh if i == 0 else []
                dbt("run", "--select", model.name, *first_refresh_batch, "--vars", json.dumps(batch_vars))


if __name__ == "__main__":
    main()
