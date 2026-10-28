{#
  Numeric id of a price series (asset dimension). Deterministic, so token and asset agree without a join.
  - beefy_db oracle series: asset_id_from_beefy_price_oracle_id(price_oracles.id)
  - beefy_api share price series (vault share tokens without a beefy_db oracle): asset_id_from_share_token(chain_id, address)
#}

{% macro asset_id_from_beefy_price_oracle_id(beefy_price_oracle_id) %}
    cityHash64('beefy_db_oracle', toString({{ beefy_price_oracle_id }}))
{%- endmacro %}

{% macro asset_id_from_share_token(chain_id, address) %}
    cityHash64('beefy_api_share', toInt64({{ chain_id }}), toString({{ address }}))
{%- endmacro %}
