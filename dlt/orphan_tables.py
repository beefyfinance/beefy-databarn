from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from lib.orphan_tables import find_orphans, manifest_is_stale, render_report, render_sql

DLT_DIR = Path(__file__).resolve().parent
DBT_DIR = DLT_DIR.parent / "dbt"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List ClickHouse tables nothing produces or reads any more. Read-only: never drops anything."
    )
    parser.add_argument("--stale-days", type=int, default=30, help="dlt tables not read by dbt and not loaded for this long are flagged (default 30)")
    parser.add_argument("--manifest", type=Path, default=DBT_DIR / "target" / "manifest.json")
    parser.add_argument("--sql-out", type=Path, help="also write DROP statements (for manual review and execution) to this file")
    args = parser.parse_args()

    if not args.manifest.exists():
        print(f"{args.manifest} not found. Run `make dbt compile` first.", file=sys.stderr)
        return 1
    if manifest_is_stale(args.manifest, DBT_DIR):
        print("warning: dbt files changed after the manifest was written; run `make dbt compile` or results may be wrong.", file=sys.stderr)

    orphans = find_orphans(DBT_DIR, DLT_DIR, args.manifest, args.stale_days)
    print(render_report(orphans))
    if args.sql_out and orphans:
        args.sql_out.write_text(render_sql(orphans, datetime.now(timezone.utc)))
        print(f"\nSQL written to {args.sql_out} (not executed). Review it, then run it manually.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
