{% macro calculate_price_variation(current_price, previous_price, precision=2) %}
    {#
        Macro: calculate_price_variation
        ---------------------------------
        Calculates the percentage variation between two prices.

        Parameters:
            current_price: The current/new price value
            previous_price: The previous/old price value
            precision: Number of decimal places (default: 2)

        Returns:
            Percentage variation rounded to specified precision
            NULL if either price is NULL or previous_price is 0

        Usage:
            {{ calculate_price_variation('prix_actuel', 'prix_precedent') }} as variation_pct
            {{ calculate_price_variation('new_price', 'old_price', 4) }} as variation_pct_precise

        Example:
            current_price: 1.85
            previous_price: 1.80
            Output: 2.78 (percentage increase)
    #}

    case
        when {{ previous_price }} is null then null
        when {{ current_price }} is null then null
        when {{ previous_price }} = 0 then null
        else round(
            ({{ current_price }} - {{ previous_price }}) / {{ previous_price }} * 100,
            {{ precision }}
        )
    end

{% endmacro %}


{% macro price_trend_category(variation_pct) %}
    {#
        Macro: price_trend_category
        ---------------------------
        Categorizes a price variation percentage into trend buckets.

        Parameters:
            variation_pct: The percentage variation value

        Returns:
            String category: 'Forte hausse', 'Hausse', 'Stable', 'Baisse', 'Forte baisse'

        Usage:
            {{ price_trend_category('variation_hebdo_pct') }} as tendance
    #}

    case
        when {{ variation_pct }} is null then 'Inconnu'
        when {{ variation_pct }} > 5 then 'Forte hausse'
        when {{ variation_pct }} > 1 then 'Hausse'
        when {{ variation_pct }} < -5 then 'Forte baisse'
        when {{ variation_pct }} < -1 then 'Baisse'
        else 'Stable'
    end

{% endmacro %}


{% macro price_percentile_category(percentile) %}
    {#
        Macro: price_percentile_category
        ---------------------------------
        Categorizes a price based on its percentile rank.

        Parameters:
            percentile: The percentile value (0 to 1)

        Returns:
            String category for the price level

        Usage:
            {{ price_percentile_category('percentile_national') }} as categorie_prix
    #}

    case
        when {{ percentile }} is null then 'Inconnu'
        when {{ percentile }} <= 0.1 then 'Très bon marché'
        when {{ percentile }} <= 0.25 then 'Bon marché'
        when {{ percentile }} <= 0.75 then 'Prix moyen'
        when {{ percentile }} <= 0.9 then 'Cher'
        else 'Très cher'
    end

{% endmacro %}
