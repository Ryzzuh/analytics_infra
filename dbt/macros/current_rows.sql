{#
  Current state from a change stream: the latest change per entity, with deleted entities
  dropped.

  Ordered by source_lsn rather than a timestamp, for the same reason SCD2 is: it is the only
  total order over committed changes, and several changes can share a timestamp.
#}
{% macro current_rows(relation, key_column='pk') %}
    select *
    from (
        select
            *,
            row_number() over (partition by {{ key_column }} order by source_lsn desc) as recency
        from {{ relation }}
    ) ranked
    where recency = 1
      and op <> 'd'
{% endmacro %}
