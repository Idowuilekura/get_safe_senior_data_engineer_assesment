{{ config(materialized='table') }}

with source_data as (

    select
        transaction_id,
        created_at as created_at_raw,
        amount,
        currency,
        charged_partner,
        status,
        created_at_timestamp as created_at,
        day,
        day_name,
        week_of_year,
        is_festive_season
    from {{ source('raw_transactions', 'premium_transaction') }}

)

select
    source_data.*,
    {{ dbt_utils.generate_surrogate_key(['transaction_id', 'charged_partner']) }} as transaction_partner_sk
from source_data
