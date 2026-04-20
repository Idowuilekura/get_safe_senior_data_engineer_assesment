{{ config(materialized='table') }}

{#-
Use a single documented 2024 FX assumption for the case study.
ECB annual average series EXR.A.GBP.EUR.SP00.A = 0.8466166015625 GBP per EUR for 2024.
That means GBP amounts are converted to EUR by dividing by the rate.
-#}

{% set ecb_2024_gbp_per_eur_rate = 0.8466166015625 %}

with processed_transactions as (

    select
        charged_partner as partner,
        date_trunc('month', created_at)::date as month,
        amount,
        currency
    from {{ ref('premium_transactions') }}
    where status = 'processed'

),

normalized_transactions as (

    select
        partner,
        month,
        case
            when currency = 'EUR' then amount
            when currency = 'GBP' then amount / cast('{{ ecb_2024_gbp_per_eur_rate }}' as numeric)
        end as amount_eur
    from processed_transactions

)

select
    partner,
    month,
    round(cast(sum(amount_eur) as numeric), 2) as total_premium
from normalized_transactions
group by
    partner,
    month
order by
    month desc,
    partner asc
