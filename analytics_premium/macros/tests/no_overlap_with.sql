{% test no_overlap_with(model, compare_model, key_column) %}

select
    accepted.{{ key_column }} as overlapping_key
from {{ model }} as accepted
inner join {{ compare_model }} as rejected
    on accepted.{{ key_column }} = rejected.{{ key_column }}

{% endtest %}
