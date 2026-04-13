{% macro test_freshness_hours(model, column_name, max_hours) %}
    {#
        Macro: test_freshness_hours
        ---------------------------
        Generic test to verify data freshness within specified hours.

        Parameters:
            model: The model/source to test
            column_name: The timestamp column to check
            max_hours: Maximum allowed age in hours

        Usage in schema.yml:
            tests:
              - test_freshness_hours:
                  column_name: _loaded_at
                  max_hours: 24
    #}

    select
        count(*) as stale_records
    from {{ model }}
    where {{ column_name }} < dateadd(hour, -{{ max_hours }}, current_timestamp())

{% endmacro %}


{% macro get_column_values_as_list(table, column) %}
    {#
        Macro: get_column_values_as_list
        ---------------------------------
        Returns distinct values from a column as a list.
        Useful for dynamic accepted_values tests or generating
        reference data.

        Parameters:
            table: The table/model reference
            column: The column name

        Usage:
            {% set fuel_types = get_column_values_as_list(ref('stg_carburants__prix'), 'carburant_nom') %}
    #}

    {% set query %}
        select distinct {{ column }}
        from {{ table }}
        where {{ column }} is not null
        order by {{ column }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set values = results.columns[0].values() %}
        {{ return(values) }}
    {% else %}
        {{ return([]) }}
    {% endif %}

{% endmacro %}


{% macro log_model_timing() %}
    {#
        Macro: log_model_timing
        -----------------------
        Logs execution timing for the current model.
        Call at the end of complex models for performance monitoring.

        Usage:
            {{ log_model_timing() }}
    #}

    {% if execute %}
        {{ log("Model " ~ this ~ " completed at " ~ modules.datetime.datetime.now().isoformat(), info=True) }}
    {% endif %}

{% endmacro %}
