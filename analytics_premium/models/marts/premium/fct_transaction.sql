{{
    config(
        materialized='incremental',
        unique_key='transaction_sk',
        incremental_strategy='merge'
    )
}}

with base as (

    select
        transaction_partner_sk,
        transaction_id,
        created_at_raw,
        created_at,
        amount,
        currency,
        charged_partner,
        status
    from {{ ref('premium_transactions') }}

),

partner_dim as (

    select
        partner_sk,
        partner_name
    from {{ ref('dim_partner') }}

),

final as (

    select
        b.transaction_partner_sk as transaction_sk,
        b.transaction_id,
        p.partner_sk,
        cast(to_char(b.created_at::date, 'YYYYMMDD') as bigint) as date_sk,
        b.created_at_raw,
        b.created_at,
        b.amount,
        b.currency,
        b.status
    from base b
    left join partner_dim p
        on b.charged_partner = p.partner_name

)

select *
from final
