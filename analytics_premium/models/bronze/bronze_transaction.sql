{{ config(materialized='table') }}

with source_data as (

    select *
    from {{ source('raw_transactions', 'premium_transaction') }}

)

select
    source_data.*,
    {{ dbt_utils.generate_surrogate_key(['transaction_id', 'charged_partner']) }} as sur_key
from source_data
