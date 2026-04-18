{{ config(materialized='table') }}

select
    charged_partner as partner,
    date_trunc('month', created_at)::date as month,
    sum(amount) as total_premium
from {{ ref('silver_transaction') }}
where status = 'processed'
group by
    charged_partner,
    date_trunc('month', created_at)::date
order by
    month desc
