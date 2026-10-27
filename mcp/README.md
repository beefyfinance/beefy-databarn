# mcp

MCP server that exposes the dbt project (models, ClickHouse column types, descriptions,
tests, model graph and docglow column-level lineage) to AI agents. **Metadata only**:
it never connects to ClickHouse.

It reads `manifest.json`, `catalog.json` and `docglow/.docglow-column-lineage-cache.json`
from the dbt `target/` directory and reloads them when they change, so the existing
`publish_docs.sh` run keeps it fresh with no redeploy.

## Tools

| Tool | Purpose |
| --- | --- |
| `search(query)` | Find models / columns by name or description |
| `list_models(layer?, tag?)` | Browse by layer (staging, intermediate, dimensions, marts, api) |
| `get_model(name)` | Columns + types + tests, primary key, parents, children |
| `column_lineage(model, column, direction, depth)` | Upstream sources or downstream impact of a column |
| `model_graph(name, direction, depth)` | Model-level dependency graph |
| `freshness()` | When the docs were generated |

Usage guidance is sent to clients via the server `instructions`, so updating it is a
server-side change only.

## Connect

Prod (set `MCP_DOMAIN_NAME` in `.env`; the endpoint is public):

```bash
claude mcp add --transport http databarn https://$MCP_DOMAIN_NAME/mcp
```

## Resilience

- **Last good snapshot**: artifacts are swapped in atomically; missing, half-written or
  corrupt files keep the previous data serving. `/health` (liveness) never depends on
  artifacts, `/ready` returns 503 until a snapshot has loaded.
- **Stateless HTTP**: no sessions, so restarts and replicas are invisible to clients.
- **Bounded work**: capped query length, result counts, lineage depth and nodes per
  response; reverse lineage is pre-indexed.
- **Abuse limits**: per-client token bucket in the app, plus Traefik rate limit,
  in-flight cap and body limit; uvicorn `--limit-concurrency` sheds load with 503.
- **Container**: non-root, read-only filesystem, no capabilities, memory/CPU/pid limits,
  no network needed at startup, no secrets and no database access.
- Set `MCP_AUTH_TOKEN` to require a bearer token if it should stop being public.

Local (stdio, reads `dbt/target` after `make dbt docs`):

```bash
claude mcp add databarn -e MCP_DBT_TARGET_DIR=$PWD/dbt/target -- uv run --directory mcp python server.py
```

This directory is deliberately not a Python package (no `__init__.py`) and is never put on
`PYTHONPATH`: a package named `mcp` would shadow the `mcp` library fastmcp depends on.
