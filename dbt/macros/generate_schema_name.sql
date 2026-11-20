{#
  Default dbt behaviour: custom schema = "<target_schema>_<custom>" (e.g. insurance_gold_insurance_staging).
  Company standard: custom schema naam EXACT use karo -> predictable table names
  (Power BI / Databricks dashboards me hardcoded naam toot-te nahi).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
