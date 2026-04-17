{{ config(materialized='table') }}

select distinct
    cast(to_char(created_at_timestamp::date, 'YYYYMMDD') as bigint) as date_sk,
    created_at_timestamp::date as full_date,
    year,
    month,
    day,
    day_name,
    week_of_year,
    is_festive_season
from {{ ref('silver_transaction') }}
where created_at_timestamp is not null
