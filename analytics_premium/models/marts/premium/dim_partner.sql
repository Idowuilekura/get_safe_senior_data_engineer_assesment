{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['charged_partner']) }} as partner_sk,
    charged_partner as partner_name
from {{ ref('premium_transactions') }}
where charged_partner is not null
