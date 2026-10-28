# mcp

MCP server that exposes the dbt project (models, ClickHouse column types, descriptions,
tests, model graph and docglow column-level lineage) to AI agents. **Metadata only**:
it never connects to ClickHouse.

It reads `manifest.json`, `catalog.json` and `docglow/.docglow-column-lineage-cache.json`
from the dbt `target/` directory and reloads them when they change, so the existing
`publish_docs.sh` run keeps it fresh with no redeploy.

## What agents are told (and what the server enforces)

The server is opinionated so agents recommend what people should actually use:

- **Layers and audiences** live in `layers.py`. `marts`, `dimensions` and `api` are
  *primary* (recommended, marts first). `data_quality` is *operational*. `staging` and
  `intermediate` are *internal*: hidden from `search`, `list_models`, `model_graph` and
  `column_lineage` unless the caller passes `include_internal=true` or asks for that layer.
- **Per-model override**: add `meta: {audience: primary | operational | internal}` to a
  model's yml to promote or hide it regardless of folder. Unknown folders default to primary.
- **Sources are described as raw tables**: dlt tables as `<pipeline>.<table>` (from the
  `<pipeline>___<table>` source naming) and Envio tables as `envio <Table>`. `get_model`
  returns `sources`, `uses` and `used_by` (skipping internal models), and `column_lineage`
  returns `origins` (the raw source columns).
- **Hardcoded data as an origin**: a model with `meta: {data_origin: hardcoded}` (the price
  and TVL correction models) is reported as an origin of kind `hardcoded`, next to the dlt
  tables, instead of an untraceable dead end.
- **Answers come from your dbt docs**: model and column descriptions are returned verbatim,
  so documentation quality in the yml files is what agents will say. The server instructions
  tell agents not to invent beyond them.

## Links in answers

Every model, source and tool result carries URLs so agents can cite them: the public
docglow site (`MCP_DOCS_URL`) and the GitHub repo (`MCP_REPO_URL` / `MCP_REPO_BRANCH`).
Both are configured via env and nothing is hardcoded: links depending on an unset URL are
simply omitted. In prod compose, `MCP_DOCS_URL` defaults to `https://$DBT_DOCS_WEBAPP_DOMAIN_NAME`. `get_model` returns `links.docs`,
`lineage_upstream`, `lineage_downstream`, `sql_definition` (the model's SQL on GitHub) and
`dbt_spec` (its yml: descriptions, columns, tests); sources get `docs` and `dbt_spec`; `column_lineage` / `model_graph` return a ready-made lineage view link;
`list_layers` and `freshness` return the site home, layers page, ERD and repo. The server
instructions tell agents to copy these verbatim and never guess URLs.

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
claude mcp add --transport http databarn https://$MCP_DOMAIN_NAME/
```

## Info page

`GET /` returns an info page (what this is, how to connect, tools, live model counts and
docs freshness): HTML for browsers, plain text for everything else. `GET /llms.txt` always
returns the plain-text version. MCP clients (`Accept: text/event-stream`) and all `POST`s
still reach the MCP endpoint. Source: `landing.py`; tool descriptions come from the tools'
docstrings, so they never drift.

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
