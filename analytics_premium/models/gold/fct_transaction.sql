{{
    config(
        materialized='incremental',
        unique_key='transaction_sk',
        incremental_strategy='merge'
    )
}}

with base as (

    select
        sur_key,
        transaction_id,
        created_at,
        created_at_timestamp,
        amount,
        currency,
        charged_partner,
        status
    from {{ ref('silver_transaction') }}

),

partner_dim as (

    select
        partner_sk,
        partner_name
    from {{ ref('dim_partner') }}

),

final as (

    select
        b.sur_key as transaction_sk,
        b.transaction_id,
        p.partner_sk,
        cast(to_char(b.created_at_timestamp::date, 'YYYYMMDD') as bigint) as date_sk,
        b.created_at,
        b.created_at_timestamp,
        b.amount,
        b.currency,
        b.status
    from base b
    left join partner_dim p
        on b.charged_partner = p.partner_name

)

select *
from final
