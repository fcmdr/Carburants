{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Macro: generate_schema_name
        ---------------------------
        Custom schema naming strategy for dbt models.

        In dev: Uses target.schema + custom_schema_name
        In prod: Uses only the custom_schema_name (cleaner names)
        In CI: Uses PR-specific schema prefix

        This follows dbt best practices for multi-environment deployments.
    #}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {# In production, use clean schema names #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- elif target.name == 'ci' -%}
        {# In CI, prefix with PR schema #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}

    {%- else -%}
        {# In dev, prefix with user schema #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
