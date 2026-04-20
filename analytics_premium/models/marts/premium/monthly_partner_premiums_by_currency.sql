{{ config(materialized='table') }}

select
    charged_partner as partner,
    date_trunc('month', created_at)::date as month,
    currency,
    round(cast(sum(amount) as numeric), 2) as total_premium
from {{ ref('premium_transactions') }}
where status = 'processed'
group by
    charged_partner,
    date_trunc('month', created_at)::date,
    currency
order by
    month desc,
    partner asc,
    currency asc
