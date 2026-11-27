{#
    Compare two money expressions within the project's rounding tolerance.

    Every monetary component is rounded to cents independently, so an exact
    equality test fails on perfectly correct data. Centralising the tolerance
    means a model and its test can never disagree about what "equal" means.
#}
{% macro money_matches(left, right, tolerance=none) -%}
    abs(coalesce({{ left }}, 0) - coalesce({{ right }}, 0))
        <= {{ tolerance if tolerance is not none else var('rounding_tolerance') }}
{%- endmacro %}

{#
    The inverse, for singular tests, which must return the OFFENDING rows.
#}
{% macro money_differs(left, right, tolerance=none) -%}
    abs(coalesce({{ left }}, 0) - coalesce({{ right }}, 0))
        > {{ tolerance if tolerance is not none else var('rounding_tolerance') }}
{%- endmacro %}
