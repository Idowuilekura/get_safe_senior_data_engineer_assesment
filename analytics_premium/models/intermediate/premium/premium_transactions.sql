{{
    config(
        materialized='incremental',
        unique_key='transaction_partner_sk',
        incremental_strategy='merge'
    )
}}

select
    transaction_partner_sk,
    transaction_id,
    created_at_raw,
    amount,
    currency,
    charged_partner,
    status,
    created_at,
    year,
    month,
    day,
    day_name,
    week_of_year,
    is_festive_season
from {{ ref('premium_transaction_quality') }}
where not is_rejected
