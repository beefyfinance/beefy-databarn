"""What each dbt layer is for, and who should be shown it.

Audience drives the server's defaults: `primary` models are what agents should suggest
and describe; `operational` ones only matter for data-quality questions; `internal`
ones (staging, intermediate) are implementation details, shown only when asked.
A model can override its layer's audience with `meta: {audience: primary|operational|internal}`
in its dbt yml. Unknown layers default to `primary` so new folders are never hidden by accident.
"""

LAYERS: dict[str, dict] = {
    "facts": {
        "audience": "primary",
        "rank": 0,
        "title": "Facts",
        "description": "Reusable fact tables in the fact database (fact_*): one row per observation (*_obs: "
        "harvests, buybacks, zaps, balance changes) or per entity per period (*_5m, *_1h, *_1d: prices, product and "
        "chain stats), plus *_latest views. Full history, keyed to the dimensions. Start here for almost any "
        "analytical question.",
    },
    "dimensions": {
        "audience": "primary",
        "rank": 1,
        "title": "Dimensions",
        "description": "Reference entities in the dim database (dim_*: chain, product, token, asset, platform, "
        "account). Join facts to these for names and attributes.",
    },
    "marts": {
        "audience": "primary",
        "rank": 2,
        "title": "Marts",
        "description": "Tables shaped for one dashboard or question, in the mart database (mart_*: daily revenue "
        "summary, TVL changes). Use them for that question; build anything else from the facts.",
    },
    "api": {
        "audience": "primary",
        "rank": 3,
        "title": "API views",
        "description": "Views backing the public REST API, in the api database (api_*). Stable contracts for consumers; "
        "not for ad-hoc analysis.",
    },
    "legacy": {
        "audience": "internal",
        "rank": 8,
        "title": "Legacy (deprecated)",
        "description": "Temporary views under the old analytics names (legacy_*: analytics.product_stats, ...) over "
        "the new tables, for readers that have not moved yet. Never suggest them: point to the dim/fact/mart table "
        "named in their description.",
    },
    "data_quality": {
        "audience": "operational",
        "rank": 4,
        "title": "Data quality",
        "description": "Lists of known gaps or mismatches to inspect, in the dbt database (dq_*). Only relevant when "
        "asked about data quality.",
    },
    "intermediate": {
        "audience": "internal",
        "rank": 5,
        "title": "Intermediate (internal)",
        "description": "Internal building blocks (int_*) that join and reshape staging models for the facts. "
        "Implementation detail: do not suggest these unless asked.",
    },
    "staging": {
        "audience": "internal",
        "rank": 6,
        "title": "Staging (internal)",
        "description": "Internal 1:1 cleanups of raw source tables (stg_*): renaming, typing, deduplication. "
        "Implementation detail: do not suggest these unless asked.",
    },
    "source": {
        "audience": "source",
        "rank": 7,
        "title": "Sources",
        "description": "Raw tables loaded by dlt pipelines (dlt.<pipeline>___<table>) and the Envio indexer. "
        "Refer to data origin in these terms.",
    },
}


def layer_of(node: dict) -> str:
    if node["resource_type"] == "source":
        return "source"
    parts = node["original_file_path"].split("/")  # models/<layer>/...
    return parts[1] if len(parts) > 2 else "other"


def audience_of(node: dict) -> str:
    override = (node.get("meta") or {}).get("audience")
    if override in ("primary", "operational", "internal"):
        return override
    return LAYERS.get(layer_of(node), {}).get("audience", "primary")


def rank_of(node: dict) -> int:
    return LAYERS.get(layer_of(node), {}).get("rank", 0)


def source_info(node: dict) -> dict:
    """Describe a source node as the raw table it is: a dlt pipeline table or an Envio table."""
    name, kind = node["name"], node["source_name"]
    info = {"id": f"{kind}.{name}", "kind": kind, "description": node.get("description") or None}
    if kind == "dlt":
        pipeline, _, table = name.partition("___")  # beefy_api___vaults -> beefy_api, vaults
        info.update(pipeline=pipeline, table=table or name)
    else:
        info.update(table=name)
    return info


def is_hardcoded_origin(node: dict) -> bool:
    """A model with `meta: {data_origin: hardcoded}`: data maintained by hand in dbt (e.g.
    price/TVL corrections), not loaded from a source. Treated as an origin in lineage."""
    return node.get("resource_type") == "model" and (node.get("meta") or {}).get("data_origin") == "hardcoded"
