/*
  Overrides dbt's default schema naming behaviour.
  Returns the custom schema name as-is (e.g. 'silver', 'gold') instead of 
  concatenating it with the target schema (e.g. 'bronze_silver', 'bronze_gold').
  Falls back to the target schema if no custom schema is defined.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}