{{
    config(
        materialized='incremental',
        unique_key='sur_key',
        incremental_strategy='merge'
    )
}}

select
    sur_key,
    transaction_id,
    created_at,
    amount,
    currency,
    charged_partner,
    status,
    created_at_timestamp,
    year,
    month,
    day,
    day_name,
    week_of_year,
    is_festive_season
from {{ ref('silver_transaction_quality') }}
where not is_rejected
