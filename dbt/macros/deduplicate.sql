{#
    Keep one row per natural key, preferring the newest ingestion.

    The raw layer has no unique constraint by design, so duplicates reach it.
    This is where they are removed - the staging layer is the first place the
    data is allowed to be clean. `ingestion_id` is the tie-breaker because it
    belongs to the pipeline rather than to the business data, so it is always
    present and always monotonic even when the business timestamps are not.
#}
{% macro dedupe_by(key_column, order_column='ingestion_id') -%}
    row_number() over (
        partition by {{ key_column }}
        order by {{ order_column }} desc
    ) = 1
{%- endmacro %}
