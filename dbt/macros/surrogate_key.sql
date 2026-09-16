{#
  Deterministic surrogate key. Same inputs always give the same key, so a full rebuild of a
  dimension does not invalidate keys already referenced elsewhere.

  Hand-rolled rather than pulling in dbt_utils: this is the only macro from it we would use,
  and a package dependency has to be installed before dbt can even parse the project.
#}
{% macro surrogate_key(fields) -%}
    md5(
        {%- for field in fields %}
        coalesce(cast({{ field }} as text), '<null>')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{%- endmacro %}
