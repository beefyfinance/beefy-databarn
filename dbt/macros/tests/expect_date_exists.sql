{% test expect_date_exists(model, expression, days_ago=1) %}

-- Test that a specific UTC date exists in the model.
-- Default days_ago=1 is calendar yesterday (toDate(now('UTC')) - 1).
--
-- Parameters:
--   expression: Date column or expression (e.g. date_day)
--   days_ago: Complete UTC days back to require (default: 1)

with date_check as (
    select count(*) as date_count
    from {{ model }}
    where toDate({{ expression }}) = toDate(now('UTC')) - {{ days_ago }}
),
validation_errors as (
    select
        toDate(now('UTC')) - {{ days_ago }} as expected_date,
        date_count
    from date_check
    where date_count = 0
)

select *
from validation_errors

{% endtest %}

