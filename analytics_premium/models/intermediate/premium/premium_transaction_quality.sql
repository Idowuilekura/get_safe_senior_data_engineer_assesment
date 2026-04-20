{{ config(materialized='view') }}

with base as (

    select
        transaction_partner_sk,
        transaction_id,
        created_at_raw,
        amount,
        currency,
        charged_partner,
        status,
        created_at,
        day,
        day_name,
        week_of_year,
        is_festive_season
    from {{ ref('stg_premium__transactions') }}

),

profiled as (

    select
        *,
        case
            when transaction_id is null then 0
            else count(*) over (partition by transaction_id)
        end as transaction_id_occurrences,
        count(*) over (partition by transaction_partner_sk) as transaction_partner_sk_occurrences,
        extract(year from created_at)::integer as year,
        extract(month from created_at)::integer as month
    from base

)

select
    profiled.*,
    transaction_id is null as has_null_transaction_id,
    transaction_id_occurrences > 1 as has_duplicate_transaction_id,
    transaction_partner_sk_occurrences > 1 as has_duplicate_transaction_partner_key,
    charged_partner is null as has_missing_partner,
    created_at is null as has_missing_created_at,
    amount <= 0 as has_non_positive_amount,
    (
        transaction_id is null
        or transaction_id_occurrences > 1
        or transaction_partner_sk_occurrences > 1
        or charged_partner is null
        or created_at is null
        or amount <= 0
    ) as is_rejected,
    concat_ws(
        ',',
        case when transaction_id is null then 'null_transaction_id' end,
        case when transaction_id_occurrences > 1 then 'duplicate_transaction_id' end,
        case when transaction_partner_sk_occurrences > 1 then 'duplicate_transaction_partner_key' end,
        case when charged_partner is null then 'missing_partner' end,
        case when created_at is null then 'missing_created_at' end,
        case when amount <= 0 then 'non_positive_amount' end
    ) as rejection_reason
from profiled
