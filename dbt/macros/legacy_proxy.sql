{#-
  Guard of a legacy proxy view (models/legacy/): fail when its old analytics name is still a table. The view
  materialization runs CREATE OR REPLACE VIEW over whatever exists, so building the proxy before that table was moved
  to its new database would replace it with a view and lose its data. Call it at the top of the model.
-#}
{% macro legacy_proxy_guard() -%}
  {%- if execute -%}
    {%- set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}
    {%- if existing is not none and existing.type != 'view' -%}
      {{ exceptions.raise_compiler_error(this ~ " is still a " ~ existing.type ~ ": move it to its new database before "
          ~ "building " ~ model.name ~ ", which would replace it with a view (see dbt/CONVENTIONS.md, legacy layer)") }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
