{{ config(materialized='table') }}

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
    is_festive_season,
    transaction_id_occurrences,
    sur_key_occurrences,
    has_null_transaction_id,
    has_duplicate_transaction_id,
    has_duplicate_surrogate_key,
    has_missing_partner,
    has_missing_created_at_timestamp,
    is_rejected,
    rejection_reason
from {{ ref('silver_transaction_quality') }}
where is_rejected
