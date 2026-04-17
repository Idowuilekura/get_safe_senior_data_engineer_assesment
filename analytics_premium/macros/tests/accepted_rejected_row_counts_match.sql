{% test accepted_rejected_row_counts_match(model, base_model, rejected_model) %}

with base_counts as (

    select count(*) as row_count
    from {{ base_model }}

),

accepted_counts as (

    select count(*) as row_count
    from {{ model }}

),

rejected_counts as (

    select count(*) as row_count
    from {{ rejected_model }}

)

select
    base_counts.row_count as base_row_count,
    accepted_counts.row_count as accepted_row_count,
    rejected_counts.row_count as rejected_row_count
from base_counts
cross join accepted_counts
cross join rejected_counts
where base_counts.row_count != accepted_counts.row_count + rejected_counts.row_count

{% endtest %}
