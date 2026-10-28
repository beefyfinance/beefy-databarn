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

from store import ArtifactsUnavailable, Snapshot, _short, store

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

MAX_RESULTS = 50
MAX_LINEAGE_NODES = 300  # cap on nodes returned by one lineage/graph call
MAX_QUERY_CHARS = 200
MAX_NAME_CHARS = 200
MAX_BODY_BYTES = 64 * 1024
RATE_PER_MINUTE = int(os.environ.get("MCP_RATE_PER_MINUTE", "120"))
RATE_BURST = int(os.environ.get("MCP_RATE_BURST", "30"))

INSTRUCTIONS = """\
Schema documentation for the Beefy Databarn ClickHouse warehouse (dbt project).
Layers, from raw to consumer-facing: source -> staging (stg_*) -> intermediate (int_*)
-> dimensions / marts -> api (api_*). Prefer dimensions, marts and api models for
analysis; staging and intermediate models are implementation details.

Workflow: `search` to find models or columns, `get_model` for the full schema,
description, primary key, tests and neighbours, `column_lineage` to see where a column
comes from (or what depends on it). Schemas are the live ClickHouse types from the last
`dbt docs generate`. This server has no access to table data.
"""

mcp = FastMCP("beefy-databarn", instructions=INSTRUCTIONS)

UNAVAILABLE = {"error": "Documentation is temporarily unavailable, retry shortly."}


def _snap() -> Snapshot | None:
    try:
        return store.get()
    except ArtifactsUnavailable:
        return None


def _layer(node: dict) -> str:
    parts = node["original_file_path"].split("/")
    return parts[1] if node["resource_type"] == "model" and len(parts) > 2 else "source"


def _clip(text: str, n: int) -> str:
    return text.strip()[:n]


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


@mcp.tool
def search(query: str, limit: int = 20) -> list[dict] | dict:
    """Search models, sources and columns by name or description (case-insensitive,
    all words must match). Returns matching models with the columns that matched."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    words = _clip(query, MAX_QUERY_CHARS).lower().split()[:8]
    if not words:
        return {"error": "Empty query."}
    hits = []
    for kind, name, node, head, cols in s.search_index:
        matched = [c for c, text in cols if any(w in text for w in words) and all(w in text or w in head for w in words)]
        if all(w in head for w in words) or matched:
            hits.append(
                (
                    (0 if all(w in name.lower() for w in words) else 1, len(name)),
                    {
                        "name": name,
                        "kind": kind,
                        "layer": _layer(node),
                        "description": (node.get("description") or "")[:200],
                        "matching_columns": matched[:10],
                    },
                )
            )
    hits.sort(key=lambda h: h[0])
    return [h[1] for h in hits[: max(1, min(limit, MAX_RESULTS))]]


@mcp.tool
def list_models(layer: str | None = None, tag: str | None = None) -> list[dict] | dict:
    """List models, optionally filtered by layer (staging, intermediate, dimensions,
    marts, api, data_quality, ...) or dbt tag."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    out = []
    for name, node in sorted(s.models.items()):
        if layer and _layer(node) != layer:
            continue
        if tag and tag not in node.get("tags", []):
            continue
        out.append(
            {
                "name": name,
                "layer": _layer(node),
                "materialized": node["config"].get("materialized"),
                "description": (node.get("description") or "")[:160],
            }
        )
    return out


@mcp.tool
def get_model(name: str) -> dict:
    """Full documentation for a model or source: description, ClickHouse columns with
    types/descriptions/tests, primary key, model-level tests, parents and children."""
    s = _snap()
    if s is None:
        return UNAVAILABLE
    found = s.resolve(_clip(name, MAX_NAME_CHARS))
    if not found:
        return {"error": "Unknown model. Use `search` or `list_models`."}
    uid, node = found
    cat_meta = (s.catalog.get(uid) or {}).get("metadata", {})
    return {
        "name": _short(uid),
        "kind": "model" if node["resource_type"] == "model" else "source",
        "layer": _layer(node),
        "table": f"{node.get('schema')}.{node.get('alias') or node['name']}",
        "materialized": node.get("config", {}).get("materialized") or cat_meta.get("type"),
        "description": node.get("description") or None,
        "meta": node.get("meta") or {},
        "tags": node.get("tags", []),
        "file": node["original_file_path"],
        "columns": s.columns(uid, node),
        "model_tests": [t for t in s.tests.get(uid, []) if not t["column"]],
        "parents": [_short(p) for p in node.get("depends_on", {}).get("nodes", [])],
        "children": [_short(c) for c in s.children.get(uid, [])],
    }


def _upstream(s: Snapshot, uid: str, col: str, depth: int, seen: set, budget: _Budget) -> list[dict]:
    out = []
    for e in s.lineage.get(uid, {}).get(col, []):
        if not budget.take():
            break
        key = (e["source_model"], e["source_column"])
        node = {"model": _short(e["source_model"]), "column": e["source_column"], "transformation": e["transformation"]}
        if depth > 1 and key not in seen:
            seen.add(key)
            node["upstream"] = _upstream(s, e["source_model"], e["source_column"], depth - 1, seen, budget)
        out.append(node)
    return out


def _downstream(s: Snapshot, uid: str, col: str, depth: int, seen: set, budget: _Budget) -> list[dict]:
    out = []
    for child_uid, child_col, transformation in s.reverse_lineage.get((uid, col), []):
        if not budget.take():
            break
        key = (child_uid, child_col)
        node = {"model": _short(child_uid), "column": child_col, "transformation": transformation}
        if depth > 1 and key not in seen:
            seen.add(key)
            node["downstream"] = _downstream(s, child_uid, child_col, depth - 1, seen, budget)
        out.append(node)
    return out


@mcp.tool
def column_lineage(model: str, column: str, direction: str = "upstream", depth: int = 3) -> dict:
    """Column-level lineage. direction='upstream' shows which source columns feed
    `model.column` (with transformation: passthrough/aggregated/...); 'downstream'
    shows which columns in other models are derived from it (impact analysis).
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
    depth, budget = max(1, min(depth, 8)), _Budget()
    walk = _downstream if direction == "downstream" else _upstream
    out = {"model": _short(uid), "column": column, direction: walk(s, uid, column, depth, set(), budget)}
    if budget.truncated:
        out["note"] = f"Truncated at {MAX_LINEAGE_NODES} nodes; lower `depth` or query a downstream model."
    elif not out[direction] and uid not in s.lineage:
        out["note"] = "No column lineage recorded for this model (source, or tracing failed)."
    return out


@mcp.tool
def model_graph(name: str, direction: str = "upstream", depth: int = 2) -> dict:
    """Model-level dependency graph around `name`. direction='upstream' (parents) or
    'downstream' (children)."""
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
        if direction == "downstream":
            return s.children.get(u, [])
        node = s.models.get(_short(u)) or s.sources.get(_short(u)) or {}
        return node.get("depends_on", {}).get("nodes", [])

    def walk(u: str, d: int) -> dict:
        out = {}
        if d == 0:
            return out
        for n in neighbours(u):
            if not budget.take():
                break
            out[_short(n)] = walk(n, d - 1)
        return out

    result = {"model": _short(uid), direction: walk(uid, max(1, min(depth, 6)))}
    if budget.truncated:
        result["note"] = f"Truncated at {MAX_LINEAGE_NODES} nodes; lower `depth`."
    return result


@mcp.tool
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
    }


# ---- HTTP hardening ------------------------------------------------------
_EXEMPT = {"/health", "/ready"}


class Guard:
    """Pure-ASGI protection for a public endpoint: optional bearer token, request size
    limit, and a per-client token bucket (client = address appended by the trusted proxy)."""

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
        xff = headers.get(b"x-forwarded-for", b"").decode()
        if xff:
            return xff.split(",")[-1].strip()
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
    middleware = [Middleware(Guard, token=os.environ.get("MCP_AUTH_TOKEN", ""))]
    # Stateless + JSON responses: no in-memory sessions, survives restarts/replicas.
    return mcp.http_app(path="/", middleware=middleware, stateless_http=True, json_response=True)


app = build_app()

if __name__ == "__main__":
    mcp.run(transport="stdio")
