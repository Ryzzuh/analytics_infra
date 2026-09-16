{#
  Numeric bounds. A churn score outside 0-100 means the scoring rules changed without the
  bands being revisited, which would silently mis-segment every account.
#}
{% test accepted_range(model, column_name, min=none, max=none) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  {% if min is not none %} and {{ column_name }} < {{ min }} {% endif %}
  {% if max is not none %} and {{ column_name }} > {{ max }} {% endif %}
{% endtest %}
