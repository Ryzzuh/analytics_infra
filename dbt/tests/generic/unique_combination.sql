{#
  Uniqueness across several columns. dbt's built-in `unique` covers one column only, and the
  grain of most facts here is composite. Hand-rolled rather than adding dbt_utils for one test.
#}
{% test unique_combination(model, columns) %}
select
    {{ columns | join(', ') }},
    count(*) as occurrences
from {{ model }}
group by {{ range(1, columns | length + 1) | join(', ') }}
having count(*) > 1
{% endtest %}
