"""Human/agent-readable info page served on GET / (HTML) and GET / or /llms.txt (plain text)."""
import html

import links

LAYERS = "Recommended: marts (analysis-ready facts) and dimensions (chain, product, token, ...), plus api views. Internal: staging (stg_*) and intermediate (int_*) are hidden by default. Origins are reported as raw source tables (dlt pipeline tables, Envio)."

EXAMPLES = [
    "Which models hold daily revenue, and what are their columns?",
    "Where does api_revenue_summary.revenue_usd_30d come from?",
    "What breaks if I change stg_beefy_db__feebatch_harvests.treasury_amt?",
]


def _facts(tools: list[tuple[str, str]], stats: dict | None, url: str) -> dict:
    return {
        "url": url,
        "claude_code": f"claude mcp add --transport http databarn {url}",
        "json_config": '{ "mcpServers": { "databarn": { "type": "http", "url": "%s" } } }' % url,
        "tools": [(n, " ".join((d or "").split())) for n, d in tools],
        "stats": stats,
    }


def render_text(url: str, tools, stats) -> str:
    f = _facts(tools, stats, url)
    lines = [
        "# Beefy Databarn MCP server",
        "",
        "Schema documentation for the Beefy Databarn ClickHouse warehouse (dbt project): models,",
        "column types, descriptions, tests, model graph and column-level lineage.",
        "Public, read-only, metadata only. It has no access to table data.",
        "",
        "## Connect",
        "",
        f"Endpoint (MCP streamable HTTP): {url}",
        "",
        "Claude Code:",
        f"  {f['claude_code']}",
        "",
        "Other MCP clients (Cursor, VS Code, ...): add a remote/HTTP server with the URL above, or",
        f"  {f['json_config']}",
        "",
        "## Tools",
        "",
    ]
    lines += [f"- {n}: {d}" for n, d in f["tools"]]
    shown = [(l, u) for l, u in (("Docs site", links.site()), ("Layers", links.site("layers")), ("ERD", links.site("erd")), ("Source code", links.github())) if u]
    if shown:
        lines += ["", "## Links", ""] + [f"- {l}: {u}" for l, u in shown]
    lines += ["", "## Layers", "", LAYERS, "", "## Try asking", ""]
    lines += [f"- {e}" for e in EXAMPLES]
    if stats:
        lines += [
            "",
            "## Status",
            "",
            f"{stats['models']} models, {stats['sources']} sources, {stats['lineage']} with column lineage; "
            f"docs generated {stats['generated_at']}.",
        ]
    else:
        lines += ["", "## Status", "", "Documentation artifacts are not loaded yet."]
    return "\n".join(lines) + "\n"


def render_html(url: str, tools, stats) -> str:
    f = _facts(tools, stats, url)
    e = html.escape
    rows = "".join(f"<tr><td><code>{e(n)}</code></td><td>{e(d)}</td></tr>" for n, d in f["tools"])
    ex = "".join(f"<li>{e(x)}</li>" for x in EXAMPLES)
    status = (
        f"{stats['models']} models · {stats['sources']} sources · {stats['lineage']} with column lineage"
        f"<br><small>docs generated {e(stats['generated_at'])}</small>"
        if stats
        else "Documentation artifacts are not loaded yet."
    )
    items = [(l, u) for l, u in (("Docs site", links.site()), ("Layers", links.site("layers")), ("ERD", links.site("erd")), ("Source code on GitHub", links.github())) if u]
    link_block = "<h2>Links</h2><ul>" + "".join(f'<li><a href="{e(u)}">{e(l)}</a></li>' for l, u in items) + "</ul>" if items else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Beefy Databarn MCP</title>
<style>
:root{{--bg:#fff;--fg:#1c1b1a;--muted:#6b6862;--card:#f6f4f0;--line:#e2ded7;--accent:#2b6cb0}}
@media (prefers-color-scheme:dark){{:root{{--bg:#161514;--fg:#ece9e4;--muted:#9b968d;--card:#201f1d;--line:#34312e;--accent:#7fb2ea}}}}
body{{margin:0;background:var(--bg);color:var(--fg);font:16px/1.55 system-ui,sans-serif}}
main{{max-width:760px;margin:0 auto;padding:32px 16px 64px}}
h1{{font-size:1.6rem;margin:0 0 4px}} h2{{font-size:1.1rem;margin:32px 0 8px}}
p,li{{color:var(--fg)}} .muted,small{{color:var(--muted)}}
pre,code{{font:13px ui-monospace,Menlo,monospace}}
pre{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px;overflow-x:auto;margin:8px 0}}
code{{background:var(--card);padding:1px 5px;border-radius:4px}} pre code{{background:none;padding:0}}
table{{border-collapse:collapse;width:100%}} td{{border-top:1px solid var(--line);padding:8px 8px 8px 0;vertical-align:top}}
td:first-child{{white-space:nowrap}} .status{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:10px 14px}}
a{{color:var(--accent)}}
</style></head><body><main>
<h1>Beefy Databarn MCP</h1>
<p class="muted">Schema docs for the Beefy Databarn ClickHouse warehouse (dbt): models, column types, descriptions, tests and column-level lineage. Public, read-only, metadata only. No access to table data.</p>
<div class="status">{status}</div>
<h2>Connect</h2>
<p>Endpoint (MCP streamable HTTP): <code>{e(url)}</code></p>
<p>Claude Code:</p><pre><code>{e(f['claude_code'])}</code></pre>
<p>Other MCP clients (Cursor, VS Code, ...): add a remote/HTTP server with that URL, or use:</p>
<pre><code>{e(f['json_config'])}</code></pre>
<h2>Tools</h2><table>{rows}</table>
{link_block}
<h2>Layers</h2><p>{e(LAYERS)}</p>
<h2>Try asking</h2><ul>{ex}</ul>
<p class="muted">Plain-text version of this page: <a href="/llms.txt">/llms.txt</a></p>
</main></body></html>
"""
