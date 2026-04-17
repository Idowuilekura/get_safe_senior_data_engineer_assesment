{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['charged_partner']) }} as partner_sk,
    charged_partner as partner_name
from {{ ref('silver_transaction') }}
where charged_partner is not null
