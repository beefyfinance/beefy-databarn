"""Beefy Databarn MCP server: dbt models, columns, tests and column-level lineage.

Public, read-only, metadata only: no warehouse access, no row data.
"""
import hmac
import logging
import os
import time

from fastmcp import FastMCP
from starlette.middleware import Middleware
from starlette.responses import JSONResponse, PlainTextResponse

from landing import render_html, render_text
from layers import LAYERS, audience_of, layer_of, rank_of, source_info
import links
from store import ArtifactsUnavailable, Snapshot, _short, store

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

MAX_RESULTS = 50
MAX_LINEAGE_NODES = 300  # cap on nodes returned by one lineage/graph call
MAX_QUERY_CHARS = 200
MAX_NAME_CHARS = 200
MAX_BODY_BYTES = 64 * 1024
RATE_PER_MINUTE = int(os.environ.get("MCP_RATE_PER_MINUTE", "120"))
RATE_BURST = int(os.environ.get("MCP_RATE_BURST", "30"))
# Trusted proxies in front of the app, each appending to X-Forwarded-For (Traefik = 1,
# Cloudflare + Traefik = 2). The client is the Nth entry from the right; anything a
# client puts further left is ignored, so the header can't be spoofed.
PROXY_HOPS = max(1, int(os.environ.get("MCP_TRUSTED_PROXY_HOPS", "1")))

INSTRUCTIONS = """\
Documentation for the Beefy Databarn analytics warehouse (ClickHouse, built with dbt).

HOW TO ANSWER
- Recommend MARTS first (analysis-ready facts and stats), then DIMENSIONS (chain, product,
  token, platform, account) to join to, then API views. These are the models people use.
- Do NOT suggest or describe staging (stg_*) or intermediate (int_*) models unless the user
  asks about them or about how something is built. They are internal implementation details.
  Search and list tools hide them by default; pass include_internal=true only when asked.
- Describe data origin in terms of raw SOURCE TABLES: dlt pipeline tables
  (e.g. dlt `beefy_api` pipeline, table `vaults`) or Envio indexer tables. Every model
  returned by get_model has a `sources` list; use it instead of naming internal models.
- Use each model's own description and column descriptions when explaining it; do not invent.
- ALWAYS CITE LINKS when present. Models, sources and tool results carry URLs (`docs`, `sql_definition`,
  `dbt_spec`, `lineage_*`, `links`): the public docs site (docglow) and the GitHub repo. When you mention
  a model, link its docs page, and link its `sql_definition` on GitHub when discussing how it is built, and its `dbt_spec`
  when citing its documented columns or tests.
  Copy URLs verbatim from tool results; never construct or guess them.

TOOLS
`list_layers` explains the layers. `search` finds models/columns; `get_model` gives the full
schema, tests, primary key, what a model builds on / is used by, and its source tables.
`column_lineage` traces a column to its raw source columns (or what depends on it).
Types are the live ClickHouse types from the last `dbt docs generate`. No table data is available.
"""

mcp = FastMCP("beefy-databarn", instructions=INSTRUCTIONS)

TOOLS: list[tuple[str, str]] = []  # (name, docstring) for the info page


def tool(fn):
    """Register `fn` as an MCP tool and remember its docs for the info page."""
    TOOLS.append((fn.__name__, fn.__doc__ or ""))
    return mcp.tool(fn)


UNAVAILABLE = {"error": "Documentation is temporarily unavailable, retry shortly."}
HIDDEN_NOTE = "Internal (staging/intermediate) and operational models are hidden; pass include_internal=true if the user asked about them."


def _snap() -> Snapshot | None:
    try:
        return store.get()
    except ArtifactsUnavailable:
        return None


def _clip(text: str, n: int) -> str:
    return text.strip()[:n]


def _visible(node: dict, include_internal: bool) -> bool:
    return include_internal or audience_of(node) == "primary"


def _brief(s: Snapshot, uid: str) -> dict:
    node = s.node(uid)
    return {
        "name": _short(uid),
        "layer": layer_of(node),
        "description": (node.get("description") or "")[:160],
        **links.present(docs=links.doc_url(uid)),
    }


def _sources(s: Snapshot, uid: str) -> list[dict]:
    return [_source(s, u) for u in s.origins(uid)]


def _source(s: Snapshot, uid: str) -> dict:
    return {**source_info(s.node(uid)), **links.present(docs=links.doc_url(uid))}


class _Budget:
    """Caps the number of nodes a lineage/graph response may contain."""

    def __init__(self, n: int = MAX_LINEAGE_NODES) -> None:
        self.left, self.truncated = n, False

    def take(self) -> bool:
        if self.left <= 0:
            self.truncated = True
            return False
        self.left -= 1
        return True


@tool
def list_layers() -> dict:
    """Explain the warehouse layers (marts, dimensions, api, data_quality, intermediate,
    staging, sources): what each is for, whether to recommend it, and how many models it has."""
    s = _snap()
    counts: dict[str, int] = {}
    if s:
        for n in s.models.values():
            counts[layer_of(n)] = counts.get(layer_of(n), 0) + 1
        counts["source"] = len(s.sources)
    order = sorted(LAYERS.items(), key=lambda kv: kv[1]["rank"])
    return {
        "layers": [
            {
                "layer": name,
                "title": d["title"],
                "recommend": d["audience"] == "primary",
                "audience": d["audience"],
                "description": d["description"],
                "models": counts.get(name, 0),
            }
            for name, d in order
        ],
        **({"links": ln} if (ln := links.global_links()) else {}),
    }


@tool
def search(query: str, limit: int = 20, include_internal: bool = False) -> dict:
    """Search models and columns by name or description (case-insensitive, all words must
    match). Results are ordered marts, dimensions, api. Staging/intermediate models are
    hidden unless include_internal=true (only set it if the user asks about them)."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    words = _clip(query, MAX_QUERY_CHARS).lower().split()[:8]
    if not words:
        return {"error": "Empty query."}
    hits, hidden = [], 0
    for kind, name, node, head, cols in s.search_index:
        matched = [c for c, text in cols if any(w in text for w in words) and all(w in text or w in head for w in words)]
        if not (all(w in head for w in words) or matched):
            continue
        if not _visible(node, include_internal):
            hidden += 1
            continue
        item = {
            "name": name,
            "layer": layer_of(node),
            "description": (node.get("description") or "")[:200],
            "matching_columns": matched[:10],
            **links.present(docs=links.doc_url(node["unique_id"])),
        }
        if kind == "source":
            item.update(source_info(node))
        hits.append(((rank_of(node), 0 if all(w in name.lower() for w in words) else 1, len(name)), item))
    hits.sort(key=lambda h: h[0])
    out = {"results": [h[1] for h in hits[: max(1, min(limit, MAX_RESULTS))]]}
    if hidden:
        out["hidden_internal_matches"] = hidden
        out["note"] = HIDDEN_NOTE
    return out


@tool
def list_models(layer: str | None = None, tag: str | None = None, include_internal: bool = False) -> list[dict] | dict:
    """List models: by default only the ones to recommend (marts, dimensions, api), marts
    first. Pass layer=<name> to list one specific layer (including staging/intermediate),
    or include_internal=true to include them all."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    out = []
    for name, node in s.models.items():
        if layer:
            if layer_of(node) != layer:
                continue
        elif not _visible(node, include_internal):
            continue
        if tag and tag not in node.get("tags", []):
            continue
        out.append(
            {
                "name": name,
                "layer": layer_of(node),
                "materialized": node["config"].get("materialized"),
                "description": (node.get("description") or "")[:160],
                **links.present(docs=links.doc_url(node["unique_id"])),
                "_k": (rank_of(node), name),
            }
        )
    out.sort(key=lambda m: m["_k"])
    for m in out:
        del m["_k"]
    return out


@tool
def get_model(name: str, include_internal: bool = False) -> dict:
    """Full documentation for a model or source: description, ClickHouse columns with
    types/descriptions/tests, primary key, tests, the models it builds on (`uses`) and the
    models that use it (`used_by`), and the raw source tables it derives from (`sources`).
    Internal models in between are summarised, not listed, unless include_internal=true."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    found = s.resolve(_clip(name, MAX_NAME_CHARS))
    if not found:
        return {"error": "Unknown model. Use `search` or `list_models`."}
    uid, node = found
    is_source = node["resource_type"] != "model"
    cat_meta = (s.catalog.get(uid) or {}).get("metadata", {})
    out = {
        "name": _short(uid),
        "kind": "source" if is_source else "model",
        "layer": layer_of(node),
        "audience": audience_of(node),
        "table": f"{node.get('schema')}.{node.get('alias') or node['name']}",
        "materialized": node.get("config", {}).get("materialized") or cat_meta.get("type"),
        "description": node.get("description") or None,
        "meta": {k: v for k, v in (node.get("meta") or {}).items()},
        "tags": node.get("tags", []),
        "file": node["original_file_path"],
        **({"links": ln} if (ln := links.node_links(node)) else {}),
        "columns": s.columns(uid, node),
        "model_tests": [t for t in s.tests.get(uid, []) if not t["column"]],
    }
    if is_source:
        out["source"] = source_info(node)
        out["used_by"] = [_brief(s, u) for u in s.nearest_primary(uid, "downstream")]
        return out
    out["sources"] = _sources(s, uid)
    if audience_of(node) == "primary" and not include_internal:
        out["uses"] = [_brief(s, u) for u in s.nearest_primary(uid, "upstream")]
        out["used_by"] = [_brief(s, u) for u in s.nearest_primary(uid, "downstream")]
    else:  # asked about an internal model (or explicitly wants internals): show the real neighbours
        out["parents"] = [_brief(s, u) for u in s.parents(uid)]
        out["children"] = [_brief(s, u) for u in s.children.get(uid, [])]
    return out


def _hidden(s: Snapshot, uid: str, collapse: bool) -> bool:
    """Internal models are looked through (not listed, no depth spent) when collapsing."""
    return collapse and s.audience(uid) == "internal" and not uid.startswith("source.")


def _upstream(s: Snapshot, uid: str, col: str, depth: int, seen: set, budget: _Budget, collapse: bool) -> list[dict]:
    out = []
    for e in s.lineage.get(uid, {}).get(col, []):
        if not budget.take():
            break
        src_uid, src_col = e["source_model"], e["source_column"]
        key, hidden = (src_uid, src_col), _hidden(s, src_uid, collapse)
        children = []
        if (hidden or depth > 1) and key not in seen:
            seen.add(key)
            children = _upstream(s, src_uid, src_col, depth if hidden else depth - 1, seen, budget, collapse)
        if hidden:
            out.extend(children)  # splice through the internal model
            continue
        node = _lineage_node(s, src_uid, src_col, e["transformation"])
        if children:
            node["upstream"] = children
        out.append(node)
    return out


def _downstream(s: Snapshot, uid: str, col: str, depth: int, seen: set, budget: _Budget, collapse: bool) -> list[dict]:
    out = []
    for child_uid, child_col, transformation in s.reverse_lineage.get((uid, col), []):
        if not budget.take():
            break
        key, hidden = (child_uid, child_col), _hidden(s, child_uid, collapse)
        children = []
        if (hidden or depth > 1) and key not in seen:
            seen.add(key)
            children = _downstream(s, child_uid, child_col, depth if hidden else depth - 1, seen, budget, collapse)
        if hidden:
            out.extend(children)
            continue
        node = _lineage_node(s, child_uid, child_col, transformation)
        if children:
            node["downstream"] = children
        out.append(node)
    return out


def _lineage_node(s: Snapshot, uid: str, col: str, transformation: str) -> dict:
    node = {"model": _short(uid), "column": col, "transformation": transformation}
    if uid.startswith("source."):
        info = source_info(s.node(uid))
        node["source_table"] = f"{info['kind']} {info['pipeline']}.{info['table']}" if "pipeline" in info else f"{info['kind']} {info['table']}"
    else:
        node["layer"] = layer_of(s.node(uid))
    return node


def _leaves(s: Snapshot, nodes: list[dict], key: str, acc: dict) -> None:
    for n in nodes:
        if n.get(key):
            _leaves(s, n[key], key, acc)
        elif "source_table" in n:
            acc[(n["model"], n["column"])] = {
                "source": _source(s, s.sources[n["model"]]["unique_id"]),
                "column": n["column"],
                "transformation": n["transformation"],
            }


@tool
def column_lineage(model: str, column: str, direction: str = "upstream", depth: int = 3, include_internal: bool = False) -> dict:
    """Column-level lineage. direction='upstream' traces `model.column` back to the raw source
    table columns (dlt pipeline tables / Envio) that feed it, with the transformation
    (passthrough/aggregated/...); `origins` lists just those source columns.
    direction='downstream' shows which columns of other models derive from it (impact
    analysis). Internal staging/intermediate hops are skipped unless include_internal=true.
    Coverage is best-effort: some columns are not traceable (e.g. macros, arrays)."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    if direction not in ("upstream", "downstream"):
        return {"error": "direction must be 'upstream' or 'downstream'."}
    found = s.resolve(_clip(model, MAX_NAME_CHARS))
    if not found:
        return {"error": "Unknown model."}
    uid, node = found
    column = _clip(column, MAX_NAME_CHARS)
    if column not in {c["name"] for c in s.columns(uid, node)}:
        return {"error": "Unknown column on that model."}
    depth, budget, collapse = max(1, min(depth, 8)), _Budget(), not include_internal
    walk = _downstream if direction == "downstream" else _upstream
    tree = walk(s, uid, column, depth, set(), budget, collapse)
    out = {
        "model": _short(uid),
        "column": column,
        direction: tree,
        **({"links": ln} if (ln := links.present(model=links.doc_url(uid), lineage=links.lineage_url(uid, direction, depth))) else {}),
    }
    if direction == "upstream":
        acc: dict = {}
        _leaves(s, tree, "upstream", acc)
        out["origins"] = list(acc.values())
    if budget.truncated:
        out["note"] = f"Truncated at {MAX_LINEAGE_NODES} nodes; lower `depth` or query a downstream model."
    elif not tree and uid not in s.lineage:
        out["note"] = "No column lineage recorded for this model (source, or tracing failed)."
    return out


@tool
def model_graph(name: str, direction: str = "upstream", depth: int = 2, include_internal: bool = False) -> dict:
    """Model-level dependency graph around `name`, direction 'upstream' (what it builds on)
    or 'downstream' (what uses it). Internal staging/intermediate models are skipped
    unless include_internal=true, so you see marts, dimensions, api views and source tables."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    if direction not in ("upstream", "downstream"):
        return {"error": "direction must be 'upstream' or 'downstream'."}
    found = s.resolve(_clip(name, MAX_NAME_CHARS))
    if not found:
        return {"error": "Unknown model."}
    uid, _ = found
    budget = _Budget()

    def neighbours(u: str) -> list[str]:
        return s.children.get(u, []) if direction == "downstream" else s.parents(u)

    def label(u: str) -> str:
        return f"{_short(u)} (source table)" if u.startswith("source.") else _short(u)

    def walk(u: str, d: int, seen: frozenset) -> dict:
        out: dict = {}
        if d == 0:
            return out
        for n in neighbours(u):
            if n in seen:
                continue
            if not include_internal and s.audience(n) in ("internal", "operational") and not n.startswith("source."):
                # look through the hidden model without spending depth on it
                out.update(walk(n, d, seen | {n}))
                continue
            if not budget.take():
                break
            out[label(n)] = walk(n, d - 1, seen | {n})
        return out

    result = {
        "model": _short(uid),
        direction: walk(uid, max(1, min(depth, 6)), frozenset({uid})),
        **({"links": ln} if (ln := links.present(model=links.doc_url(uid), lineage=links.lineage_url(uid, direction, depth))) else {}),
    }
    if budget.truncated:
        result["note"] = f"Truncated at {MAX_LINEAGE_NODES} nodes; lower `depth`."
    return result


@tool
def freshness() -> dict:
    """When the documentation artifacts were generated and how many models are indexed."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    return {
        "generated_at": s.generated_at,
        "dbt_version": s.dbt_version,
        "models": len(s.models),
        "sources": len(s.sources),
        "models_with_column_lineage": len(s.lineage),
        **({"links": ln} if (ln := links.global_links()) else {}),
    }


# ---- HTTP hardening ------------------------------------------------------
_EXEMPT = {"/health", "/ready"}


class Guard:
    """Pure-ASGI protection for a public endpoint: optional bearer token, request size
    limit, and a per-client token bucket (client = Nth-from-right X-Forwarded-For entry)."""

    def __init__(self, app, token: str = "") -> None:
        self.app, self.token = app, token
        self.buckets: dict[str, tuple[float, float]] = {}  # ip -> (tokens, last_ts)

    def _allow(self, ip: str) -> bool:
        now = time.monotonic()
        tokens, last = self.buckets.get(ip, (float(RATE_BURST), now))
        tokens = min(float(RATE_BURST), tokens + (now - last) * RATE_PER_MINUTE / 60)
        if len(self.buckets) > 10_000:  # bound memory under address-spraying
            self.buckets = {k: v for k, v in self.buckets.items() if now - v[1] < 120}
        if tokens < 1:
            self.buckets[ip] = (tokens, now)
            return False
        self.buckets[ip] = (tokens - 1, now)
        return True

    @staticmethod
    def _client_ip(scope) -> str:
        headers = dict(scope["headers"])
        parts = [p.strip() for p in headers.get(b"x-forwarded-for", b"").decode().split(",") if p.strip()]
        if parts:
            return parts[-min(PROXY_HOPS, len(parts))]
        return (scope.get("client") or ("unknown", 0))[0]

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] in _EXEMPT:
            return await self.app(scope, receive, send)
        headers = dict(scope["headers"])
        if int(headers.get(b"content-length", b"0") or 0) > MAX_BODY_BYTES:
            return await JSONResponse({"error": "request too large"}, status_code=413)(scope, receive, send)
        if self.token and not hmac.compare_digest(headers.get(b"authorization", b"").decode(), f"Bearer {self.token}"):
            return await JSONResponse({"error": "unauthorized"}, status_code=401)(scope, receive, send)
        if not self._allow(self._client_ip(scope)):
            return await JSONResponse({"error": "rate limited"}, status_code=429, headers={"Retry-After": "5"})(scope, receive, send)
        await self.app(scope, receive, send)


class Landing:
    """GET / -> info page (HTML for browsers, plain text otherwise); GET /llms.txt -> plain text.
    MCP clients that ask for text/event-stream still reach the MCP endpoint."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "GET" or scope["path"] not in ("/", "/llms.txt"):
            return await self.app(scope, receive, send)
        headers = dict(scope["headers"])
        accept = headers.get(b"accept", b"").decode()
        if scope["path"] == "/" and "text/event-stream" in accept:
            return await self.app(scope, receive, send)
        host = headers.get(b"x-forwarded-host", headers.get(b"host", b"localhost")).decode().split(",")[0].strip()
        proto = headers.get(b"x-forwarded-proto", b"https").decode().split(",")[0].strip()
        url = f"{proto if proto in ('http', 'https') else 'https'}://{host}/"
        snap = _snap()
        stats = (
            {"models": len(snap.models), "sources": len(snap.sources), "lineage": len(snap.lineage), "generated_at": snap.generated_at}
            if snap
            else None
        )
        wants_html = scope["path"] == "/" and "text/html" in accept
        body, ctype = (render_html if wants_html else render_text)(url, TOOLS, stats), "text/html" if wants_html else "text/plain"
        data = body.encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", f"{ctype}; charset=utf-8".encode()),
                    (b"content-length", str(len(data)).encode()),
                    (b"cache-control", b"public, max-age=60"),
                    (b"vary", b"Accept"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": data})


@mcp.custom_route("/health", methods=["GET"])
async def health(_request):
    """Liveness: process is up. Deliberately independent of artifacts so a missing
    manifest never causes a restart loop."""
    return PlainTextResponse("ok")


@mcp.custom_route("/ready", methods=["GET"])
async def ready(_request):
    _snap()
    return PlainTextResponse("ready" if store.ready else "no artifacts loaded", status_code=200 if store.ready else 503)


def build_app():
    # Public by default. Set MCP_AUTH_TOKEN to require a bearer token.
    middleware = [Middleware(Guard, token=os.environ.get("MCP_AUTH_TOKEN", "")), Middleware(Landing)]
    # Stateless + JSON responses: no in-memory sessions, survives restarts/replicas.
    return mcp.http_app(path="/", middleware=middleware, stateless_http=True, json_response=True)


app = build_app()

if __name__ == "__main__":
    mcp.run(transport="stdio")
