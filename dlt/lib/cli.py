from __future__ import annotations

import lib.dlt_clickhouse_get_stored_state_patch  # noqa: F401

from dataclasses import dataclass
import sys
from typing import Any, Optional

import logging
logger = logging.getLogger(__name__)

@dataclass
class CliArgs:
    show_list: bool = False
    only_resource: Optional[str] = None
    loop: bool = False


def _parse_args() -> CliArgs:
    """Parse command line arguments."""
    # first arg is either a resource name or no args at all
    if len(sys.argv) == 1:
        return CliArgs()
    else:
        only_resource = sys.argv[1]
        show_list = False
        loop = False
        if len(sys.argv) == 3:
            if sys.argv[2] == "--list":
                show_list = True
            if sys.argv[2] == "--loop":
                loop = True
        return CliArgs(only_resource=only_resource, show_list=show_list, loop=loop)


def _apply_args_to_source(source: Any, args: CliArgs) -> Any:
    if args.show_list:
        print(source.resources.keys())
        sys.exit(0)

    """Apply command line arguments to a source."""
    if args.only_resource:
        # make sure the resource exists
        if args.only_resource not in source.resources.keys():
            raise ValueError(f"Resource {args.only_resource} not found in source {source.name}")
        source = source.with_resources(args.only_resource)

    return source


def _should_loop(source: Any, args: CliArgs, previous_load_info: Any) -> bool:
    "loop the source if there is only one incremental resource and the user has requested it"

    if args.loop and args.only_resource:
        if len(source.selected_resources.keys()) == 1:
            if source.selected_resources[args.only_resource].incremental:
                if previous_load_info.is_empty:
                    logger.info("%s loop completed (reached end of data)", source.name)
                    return False
                return True

    return False



async def run_pipeline_loop(pipeline: dlt.Pipeline, source_config: Any) -> Any:
    args = _parse_args()

    
    while True:
        source = _apply_args_to_source(source_config, args)
        load_info = pipeline.run(source)
        print(load_info)
        if not _should_loop(source, args, load_info):
            break

    return load_info