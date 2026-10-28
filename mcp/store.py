"""In-memory index over dbt artifacts (manifest, catalog, docglow column lineage).

Resilience rules:
- Artifacts are re-read when their mtime changes (checked at most every CHECK_INTERVAL s).
- A new snapshot is built off to the side and swapped in atomically; if the files are
  half-written, corrupt or missing, the last good snapshot keeps serving.
"""
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

from layers import audience_of, is_hardcoded_origin

logger = logging.getLogger("mcp.store")

TARGET_DIR = Path(os.environ.get("MCP_DBT_TARGET_DIR", "/app/dbt/target"))
LINEAGE_FILE = TARGET_DIR / "docglow" / ".docglow-column-lineage-cache.json"
CHECK_INTERVAL = float(os.environ.get("MCP_RELOAD_CHECK_SECONDS", "5"))


class ArtifactsUnavailable(Exception):
    """No valid snapshot has ever been loaded."""


def _short(unique_id: str) -> str:
    """model.beefy_databarn.chain -> chain; source.beefy_databarn.dlt.vaults -> dlt.vaults"""
    parts = unique_id.split(".")
    return ".".join(parts[2:]) if parts[0] == "source" else parts[-1]


@dataclass
class Snapshot:
    models: dict[str, dict]
    sources: dict[str, dict]
    catalog: dict[str, dict]
    lineage: dict[str, dict]
    children: dict[str, list[str]]
    tests: dict[str, list[dict]]
    generated_at: str
    dbt_version: str
    # (model uid, column) -> [(child uid, child column, transformation)]
    reverse_lineage: dict[tuple[str, str], list[tuple[str, str, str]]] = field(default_factory=dict)
    # [(kind, name, node, lowercase head text, [(column, lowercase text)])]
    search_index: list = field(default_factory=list)
    _origins: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for child_uid, cols in self.lineage.items():
            for child_col, edges in cols.items():
                for e in edges:
                    key = (e["source_model"], e["source_column"])
                    self.reverse_lineage.setdefault(key, []).append(
                        (child_uid, child_col, e["transformation"])
                    )
        for kind, table in (("model", self.models), ("source", self.sources)):
            for name, node in table.items():
                cols = self.columns(node["unique_id"], node)
                self.search_index.append(
                    (
                        kind,
                        name,
                        node,
                        f"{name} {node.get('description') or ''}".lower(),
                        [(c["name"], f"{c['name']} {c['description'] or ''}".lower()) for c in cols],
                    )
                )

    def node(self, uid: str) -> dict:
        return self.models.get(_short(uid)) or self.sources.get(_short(uid)) or {}

    def parents(self, uid: str) -> list[str]:
        return self.node(uid).get("depends_on", {}).get("nodes", [])

    def audience(self, uid: str) -> str:
        return audience_of(self.node(uid)) if self.node(uid) else "internal"

    def nearest_primary(self, uid: str, direction: str) -> list[str]:
        """Closest `primary` models up- or downstream, looking through internal/operational ones."""
        step = self.parents if direction == "upstream" else (lambda u: self.children.get(u, []))
        found, seen, stack = [], {uid}, list(step(uid))
        while stack:
            u = stack.pop()
            if u in seen:
                continue
            seen.add(u)
            if self.audience(u) == "primary":
                found.append(u)
            elif not u.startswith("source."):
                stack.extend(step(u))
        return sorted(found, key=_short)

    def origins(self, uid: str) -> list[str]:
        """Raw source tables (and hardcoded-data models) a model ultimately derives from; dlt load-metadata tables excluded."""
        if uid not in self._origins:
            found, seen, stack = set(), set(), [uid]
            while stack:
                u = stack.pop()
                if u in seen:
                    continue
                seen.add(u)
                if u.startswith("source."):
                    if not self.node(u).get("name", "").split("___")[-1].startswith("_dlt"):
                        found.add(u)
                elif is_hardcoded_origin(self.node(u)):
                    found.add(u)  # manual data kept in dbt: an origin, nothing further up
                else:
                    stack.extend(self.parents(u))
            self._origins[uid] = sorted(found, key=_short)
        return self._origins[uid]

    def resolve(self, name: str) -> tuple[str, dict] | None:
        """Find a model or source by short name, schema-qualified name or unique_id."""
        name = name.strip().removeprefix("analytics.")
        for table in (self.models, self.sources):
            if name in table:
                return table[name]["unique_id"], table[name]
        for table in (self.models, self.sources):
            for node in table.values():
                if node["unique_id"] == name:
                    return name, node
        return None

    def columns(self, uid: str, node: dict) -> list[dict]:
        """Merge yml-declared columns with the live types from catalog.json."""
        cat_cols = (self.catalog.get(uid) or {}).get("columns", {})
        declared = node.get("columns", {})
        out = []
        for name in dict.fromkeys([*cat_cols, *declared]):
            d, c = declared.get(name, {}), cat_cols.get(name, {})
            out.append(
                {
                    "name": name,
                    "type": c.get("type") or d.get("data_type"),
                    "description": d.get("description") or None,
                    "tests": [t["name"] for t in self.tests.get(uid, []) if t["column"] == name],
                }
            )
        return out


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _build(manifest_path: Path, catalog_path: Path, lineage_path: Path) -> Snapshot:
    manifest = _read_json(manifest_path)
    models = {_short(uid): n for uid, n in manifest["nodes"].items() if n["resource_type"] == "model"}
    sources = {_short(uid): n for uid, n in manifest["sources"].items()}

    children: dict[str, list[str]] = {}
    for uid, n in manifest["nodes"].items():
        if n["resource_type"] == "model":
            for parent in n["depends_on"]["nodes"]:
                children.setdefault(parent, []).append(uid)

    tests: dict[str, list[dict]] = {}
    for n in manifest["nodes"].values():
        if n["resource_type"] != "test" or not n.get("attached_node"):
            continue
        meta = n.get("test_metadata") or {}
        kwargs = {k: v for k, v in (meta.get("kwargs") or {}).items() if k not in ("model", "column_name")}
        tests.setdefault(n["attached_node"], []).append(
            {"name": meta.get("name") or n["name"], "column": n.get("column_name"), "args": kwargs.get("arguments", kwargs)}
        )

    # catalog and lineage are optional enrichments: a bad one must not block the rest
    catalog: dict[str, dict] = {}
    try:
        if catalog_path.exists():
            cat = _read_json(catalog_path)
            catalog = {**cat.get("nodes", {}), **cat.get("sources", {})}
    except (OSError, ValueError):
        logger.exception("catalog.json unreadable, continuing without live types")
    lineage: dict[str, dict] = {}
    try:
        if lineage_path.exists():
            raw = _read_json(lineage_path)
            lineage = {k: v["lineage"] for k, v in raw.items() if k != "__cache_meta__"}
    except (OSError, ValueError, KeyError, TypeError):
        logger.exception("column lineage cache unreadable, continuing without it")

    return Snapshot(
        models=models,
        sources=sources,
        catalog=catalog,
        lineage=lineage,
        children=children,
        tests=tests,
        generated_at=manifest["metadata"].get("generated_at", ""),
        dbt_version=manifest["metadata"].get("dbt_version", ""),
    )


class Store:
    def __init__(self) -> None:
        self._snapshot: Snapshot | None = None
        self._mtimes: tuple[float, ...] = ()
        self._failed_mtimes: tuple[float, ...] | None = None  # don't re-log/retry until files change
        self._last_check = 0.0

    def get(self) -> Snapshot:
        """Current snapshot, refreshing from disk if due. Never raises once a snapshot exists."""
        now = time.monotonic()
        if self._snapshot is None or now - self._last_check >= CHECK_INTERVAL:
            self._last_check = now
            self._refresh()
        if self._snapshot is None:
            raise ArtifactsUnavailable
        return self._snapshot

    def _refresh(self) -> None:
        paths = [TARGET_DIR / "manifest.json", TARGET_DIR / "catalog.json", LINEAGE_FILE]
        try:
            mtimes = tuple(p.stat().st_mtime if p.exists() else 0.0 for p in paths)
            if mtimes == self._failed_mtimes or (mtimes == self._mtimes and self._snapshot is not None):
                return
            snapshot = _build(*paths)
        except Exception:  # noqa: BLE001 - any failure must leave the last good snapshot in place
            self._failed_mtimes = mtimes if "mtimes" in locals() else None
            logger.exception("artifact reload failed; %s", "serving last good snapshot" if self._snapshot else "no snapshot yet")
            return
        self._snapshot, self._mtimes, self._failed_mtimes = snapshot, mtimes, None
        logger.info("loaded %d models, %d sources (generated_at=%s)", len(snapshot.models), len(snapshot.sources), snapshot.generated_at)

    @property
    def ready(self) -> bool:
        return self._snapshot is not None


store = Store()
