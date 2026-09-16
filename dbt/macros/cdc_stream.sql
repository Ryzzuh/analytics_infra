{#
  Shared front half of every CDC staging model: pick one stream out of the change log and drop
  redeliveries.

  Deduplication is on (pk, source_lsn) because an LSN identifies exactly one committed change,
  so two rows sharing one is always a redelivery and never two real changes.

  Typing stays in the individual staging models: the columns differ per table, and burying them
  in a macro would hide the one part worth reading.
#}
{% macro cdc_stream(source_table) %}
    with deduped as (
        select
            *,
            row_number() over (
                partition by pk, source_lsn
                order by loaded_at, kafka_offset
            ) as arrival_rank
        from {{ source('raw', 'cdc_changes') }}
        where source_table = '{{ source_table }}'
    )

    select
        pk,
        op,
        is_snapshot,
        source_lsn,
        source_ts,
        effective_at,
        loaded_at,
        source_path,
        -- For a delete the values live in `before`: `after` is null by definition.
        coalesce(after, before) as row_state,
        before
    from deduped
    where arrival_rank = 1
{% endmacro %}
