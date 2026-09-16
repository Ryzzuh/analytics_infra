{#
  The end of an open SCD2 interval.

  Not 'infinity': Postgres handles it correctly, but no Python client can read it back, because
  datetime has no infinity and psycopg raises DataError. Reverse ETL and the Console read this
  column, so the value has to survive the trip.

  Not 9999-12-31 23:59:59 either: rendered in a session east of UTC that rolls into year 10000
  and overflows the same way. Midnight UTC on the last day of year 9999 stays inside year 9999
  at every real offset (max +14), so it is safe wherever it is read.
#}
{% macro end_of_time() -%}
    '9999-12-31 00:00:00+00'::timestamptz
{%- endmacro %}
