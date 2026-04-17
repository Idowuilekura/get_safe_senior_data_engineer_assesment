{{ config(materialized='view') }}

with base as (

    select
        sur_key,
        transaction_id,
        created_at,
        amount,
        currency,
        charged_partner,
        status,
        created_at_timestamp,
        day,
        day_name,
        week_of_year,
        is_festive_season
    from {{ ref('bronze_transaction') }}

),

profiled as (

    select
        *,
        case
            when transaction_id is null then 0
            else count(*) over (partition by transaction_id)
        end as transaction_id_occurrences,
        count(*) over (partition by sur_key) as sur_key_occurrences,
        extract(year from created_at_timestamp)::integer as year,
        extract(month from created_at_timestamp)::integer as month
    from base

)

select
    profiled.*,
    transaction_id is null as has_null_transaction_id,
    transaction_id_occurrences > 1 as has_duplicate_transaction_id,
    sur_key_occurrences > 1 as has_duplicate_surrogate_key,
    charged_partner is null as has_missing_partner,
    created_at_timestamp is null as has_missing_created_at_timestamp,
    (
        transaction_id is null
        or transaction_id_occurrences > 1
        or sur_key_occurrences > 1
        or charged_partner is null
        or created_at_timestamp is null
    ) as is_rejected,
    concat_ws(
        ',',
        case when transaction_id is null then 'null_transaction_id' end,
        case when transaction_id_occurrences > 1 then 'duplicate_transaction_id' end,
        case when sur_key_occurrences > 1 then 'duplicate_surrogate_key' end,
        case when charged_partner is null then 'missing_partner' end,
        case when created_at_timestamp is null then 'missing_created_at_timestamp' end
    ) as rejection_reason
from profiled
