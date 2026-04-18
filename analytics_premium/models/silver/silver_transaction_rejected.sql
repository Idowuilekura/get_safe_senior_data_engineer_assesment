{{ config(materialized='table') }}

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
    is_festive_season,
    transaction_id_occurrences,
    transaction_partner_sk_occurrences,
    has_null_transaction_id,
    has_duplicate_transaction_id,
    has_duplicate_transaction_partner_key,
    has_missing_partner,
    has_missing_created_at,
    is_rejected,
    rejection_reason
from {{ ref('silver_transaction_quality') }}
where is_rejected
