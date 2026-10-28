"""Stable URLs to the public docglow site and the GitHub repo, so answers can cite them.

Docglow routes (hash router): /layers, /erd, /health, /search, /model/:id, /source/:id,
/lineage?pins=<unique_id>&depth=<n>&dir=upstream|downstream, where :id is the dbt unique_id.
"""
import os
from urllib.parse import quote

# Deployment-specific, so configured via env (nothing hardcoded in the repo). When a URL is
# not set, the links that depend on it are simply left out of tool results.
DOCS = os.environ.get("MCP_DOCS_URL", "").strip().rstrip("/")  # public docglow site
REPO = os.environ.get("MCP_REPO_URL", "").strip().rstrip("/")  # e.g. https://github.com/<org>/<repo>
BRANCH = os.environ.get("MCP_REPO_BRANCH", "main")
DBT_DIR = "dbt"  # dbt project root inside the repo


def present(**kv: str | None) -> dict:
    """The given links, minus any that are not configured."""
    return {k: v for k, v in kv.items() if v}


def site(path: str = "") -> str | None:
    return f"{DOCS}/#/{path}" if DOCS else None


def github(path: str = "") -> str | None:
    if not REPO:
        return None
    return f"{REPO}/blob/{BRANCH}/{path}" if path else REPO


def github_tree(path: str) -> str | None:
    return f"{REPO}/tree/{BRANCH}/{path}" if REPO else None


def doc_url(uid: str) -> str | None:
    kind = "source" if uid.startswith("source.") else "model"
    return site(f"{kind}/{quote(uid, safe='.')}")


def lineage_url(uid: str, direction: str = "upstream", depth: int = 2) -> str | None:
    depth = max(1, min(depth, 6))  # docglow clamps depth to 1..6
    return site(f"lineage?pins={quote(uid, safe='.')}&depth={depth}&dir={direction}")


def node_links(node: dict) -> dict:
    """Links for a model or source node: docs page, lineage views, and the files on GitHub
    (`sql_definition` = the model's SQL, `dbt_spec` = the yml with its descriptions, columns and tests)."""
    uid = node["unique_id"]
    if node["resource_type"] != "model":  # source: defined in a sources yml
        return present(docs=doc_url(uid), dbt_spec=github(f"{DBT_DIR}/{node['original_file_path']}"))
    patch = (node.get("patch_path") or "").split("://", 1)[-1]
    return present(
        docs=doc_url(uid),
        lineage_upstream=lineage_url(uid, "upstream"),
        lineage_downstream=lineage_url(uid, "downstream"),
        sql_definition=github(f"{DBT_DIR}/{node['original_file_path']}"),
        dbt_spec=github(f"{DBT_DIR}/{patch}") if patch else None,
    )


def global_links() -> dict:
    return present(
        docs_home=site(),
        layers=site("layers"),
        erd=site("erd"),
        health=site("health"),
        repo=github(),
        dbt_project=github_tree(DBT_DIR),
    )
