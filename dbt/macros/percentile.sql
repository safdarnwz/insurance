{#
  Cross-database median / percentile. DuckDB aur Databricks ka syntax alag hai ->
  adapter.dispatch se har engine ka apna version. (dbt ka standard cross-db pattern)
#}
{% macro percentile(column, p) -%}
    {{ return(adapter.dispatch('percentile')(column, p)) }}
{%- endmacro %}

{% macro default__percentile(column, p) -%}
    percentile_cont({{ p }}) within group (order by {{ column }})
{%- endmacro %}

{% macro duckdb__percentile(column, p) -%}
    quantile_cont({{ column }}, {{ p }})
{%- endmacro %}

{% macro databricks__percentile(column, p) -%}
    percentile_approx({{ column }}, {{ p }})
{%- endmacro %}
