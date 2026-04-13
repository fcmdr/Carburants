{% macro clean_coordinates(column_name) %}
    {#
        Macro: clean_coordinates
        ------------------------
        Converts raw coordinate values from the French fuel prices API
        to proper decimal degrees.

        The API returns coordinates multiplied by 100,000 (5 decimal places
        encoded as integers). This macro:
        1. Tries to parse the value as a number
        2. Divides by 100,000 to get proper decimal degrees
        3. Returns NULL if parsing fails

        Usage:
            {{ clean_coordinates('latitude') }} as latitude
            {{ clean_coordinates('longitude') }} as longitude

        Example:
            Input:  4859912 (raw latitude)
            Output: 48.59912 (decimal degrees)
    #}

    case
        when try_to_decimal({{ column_name }}, 15, 5) is not null
            then try_to_decimal({{ column_name }}, 15, 5) / 100000
        else null
    end

{% endmacro %}
