{#
  Deterministic surrogate key = md5(col1 || '|' || col2 ...).
  KYUN hash (auto-increment ki jagah): rerun/full-refresh par same key milti hai,
  parallel loads me conflict nahi, aur DuckDB + Databricks dono me same result.
  NULL ko '_null_' banate hain warna concat poora NULL ho jaata hai.
#}
{% macro surrogate_key(columns) -%}
    md5(
    {%- for c in columns -%}
        coalesce(cast({{ c }} as {{ dbt.type_string() }}), '_null_')
        {%- if not loop.last %} || '|' || {% endif -%}
    {%- endfor -%}
    )
{%- endmacro %}
