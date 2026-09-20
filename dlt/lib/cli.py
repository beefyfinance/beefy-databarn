from __future__ import annotations

import lib.dlt_clickhouse_get_stored_state_patch  # noqa: F401
import lib.snapshot_incremental  # noqa: F401

from dataclasses import dataclass
import argparse
import sys
from typing import Any, Optional

import logging

from lib.reimport import prepare_reimport
from lib.clickhouse import ensure_table_engines

logger = logging.getLogger(__name__)

@dataclass
class CliArgs:
    show_list: bool = False
    only_resource: Optional[str] = None
    loop: bool = False
    reimport: Optional[str] = None


def _parse_args() -> CliArgs:
    parser = argparse.ArgumentParser(
        description="Run a dlt pipeline",
        epilog=(
            "examples:\n"
            "  %(prog)s harvests --loop\n"
            "  %(prog)s harvests --reimport 3m\n"
            "  %(prog)s harvests --reimport 2026-06-16\n"
            "  %(prog)s harvests --reimport\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("resource", nargs="?", help="Resource to run (default: all)")
    parser.add_argument("--list", action="store_true", help="List resources and exit")
    parser.add_argument("--loop", action="store_true", help="Repeat until the incremental load is empty")
    parser.add_argument(
        "--reimport",
        nargs="?",
        const="full",
        metavar="SINCE",
        help=(
            "Rewind incremental state and re-append. Never truncates. "
            "SINCE is YYYY-MM-DD, 90d, or 3m; omit for full history. Implies --loop."
        ),
    )
    parsed = parser.parse_args()
    if parsed.reimport is not None and not parsed.resource:
        parser.error("--reimport requires a resource name")
    return CliArgs(
        show_list=parsed.list,
        only_resource=parsed.resource,
        loop=parsed.loop or parsed.reimport is not None,
        reimport=parsed.reimport,
    )


def _apply_args_to_source(source: Any, args: CliArgs) -> Any:
    if args.show_list:
        print(source.resources.keys())
        sys.exit(0)

    if args.only_resource:
        if args.only_resource not in source.resources.keys():
            raise ValueError(f"Resource {args.only_resource} not found in source {source.name}")
        source = source.with_resources(args.only_resource)

    return source


def _should_loop(source: Any, args: CliArgs, previous_load_info: Any) -> bool:
    "loop the source if there is only one incremental resource and the user has requested it"

    if not args.loop or not args.only_resource:
        return False
    if len(source.selected_resources.keys()) != 1:
        return False
    resource = source.selected_resources[args.only_resource]
    if args.reimport is None and not resource.incremental:
        return False
    if previous_load_info.is_empty:
        print(f"{args.only_resource} loop completed (reached end of data)")
        return False
    return True


def _rewrite_unsupported_merge_strategies(pipeline: Any) -> None:
    """Rewrite stored upsert tables to append so official dlt can extract them.

    Destination schemas from the ClickHouse upsert fork still have
    ``write_disposition=merge`` and ``x-merge-strategy=upsert``. dlt 1.30
    rejects that on extract even when the live resource is append.
    """
    for name in list(pipeline.schemas):
        schema = pipeline.schemas[name]
        changed = False
        for table in schema.tables.values():
            if table.get("x-merge-strategy") != "upsert":
                continue
            table["write_disposition"] = "append"
            table.pop("x-merge-strategy", None)
            changed = True
            logger.info("Rewrote %s.%s from upsert merge to append", name, table["name"])
        if changed:
            pipeline.schemas.save_schema(schema)


async def run_pipeline_loop(pipeline: Any, source_config: Any) -> Any:
    args = _parse_args()

    if args.reimport is not None:
        prepare_reimport(pipeline, source_config, args.only_resource, args.reimport)

    if pipeline.config.restore_from_destination:
        pipeline.sync_destination()
        # run() would restore again and bring upsert schemas back
        pipeline._state_restored = True
    _rewrite_unsupported_merge_strategies(pipeline)

    iteration = 0
    while True:
        iteration += 1
        if args.loop:
            print(f"Pipeline iteration {iteration}")
        source = _apply_args_to_source(source_config, args)
        declared: dict[str, str] = {}
        for name, resource in source.selected_resources.items():
            engine = resource.compute_table_schema().get("x-table-engine-type")
            if engine:
                declared[name] = str(engine)
        await ensure_table_engines(pipeline.dataset_name, declared)
        load_info = pipeline.run(source)
        print(load_info)
        if not _should_loop(source, args, load_info):
            break

    return load_info
