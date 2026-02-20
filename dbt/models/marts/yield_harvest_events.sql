{{
  config(
    materialized='view',
    tags=['intermediate', 'yield']
  )
}}

-- Mart model: Transform harvest events into yield structure with dimension joins
-- This model enriches the cleaned yield data from int_yield with chain and product dimensions
-- Uses the int_yield intermediate model which contains all yield business logic

SELECT
  cy.txn_timestamp,
  cy.chain_id as chain_id,
  dc.chain_name,
  cy.product_address,
  cy.block_number,
  cy.txn_idx,
  cy.event_idx,
  cy.txn_hash,
  cy.underlying_amount_compounded,
  cy.underlying_token_price_usd,
  cy.underlying_amount_compounded_usd
FROM {{ ref('int_yield') }} cy
INNER JOIN {{ ref('chain') }} dc
  ON cy.chain_id = dc.chain_id

