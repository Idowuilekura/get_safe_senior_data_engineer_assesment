{{ config(materialized='table') }}

select
    charged_partner as partner,
    date_trunc('month', created_at)::date as month_start_date,
    sum(amount) as total_premium
from {{ ref('silver_transaction') }}
where status = 'processed'
group by
    charged_partner,
    date_trunc('month', created_at)::date
order by
    month_start_date desc
