/*
    Analysis: Price Anomaly Detection
    -----------------------------------
    Identifies stations with unusual price behavior that may indicate
    data quality issues or exceptional pricing strategies.

    Use this analysis to:
    - Find stations with prices significantly different from regional average
    - Detect sudden price jumps that may be data entry errors
    - Identify stations that haven't updated prices recently

    Run with: dbt compile --select analysis.price_anomaly_detection
    Then execute the compiled SQL in Snowflake.
*/

with regional_stats as (
    select
        region,
        carburant_nom,
        date_prix,
        avg(prix_euros) as prix_moyen,
        stddev(prix_euros) as prix_stddev
    from {{ ref('fct_prix_carburants') }}
    where date_prix >= dateadd(day, -7, current_date())
    group by region, carburant_nom, date_prix
),

station_prices as (
    select
        f.station_id,
        s.ville,
        s.adresse,
        f.region,
        f.carburant_nom,
        f.date_prix,
        f.prix_euros,
        f.variation_jour_pct,
        r.prix_moyen as regional_avg,
        r.prix_stddev as regional_stddev,
        (f.prix_euros - r.prix_moyen) / nullif(r.prix_stddev, 0) as z_score
    from {{ ref('fct_prix_carburants') }} f
    join {{ ref('dim_stations') }} s on f.station_id = s.station_id
    join regional_stats r
        on f.region = r.region
        and f.carburant_nom = r.carburant_nom
        and f.date_prix = r.date_prix
    where f.date_prix >= dateadd(day, -7, current_date())
)

select
    station_id,
    ville,
    adresse,
    region,
    carburant_nom,
    date_prix,
    prix_euros,
    regional_avg,
    round(z_score, 2) as z_score,
    round(variation_jour_pct, 2) as variation_jour_pct,
    case
        when abs(z_score) > 3 then 'CRITICAL: Price anomaly (>3 std dev)'
        when abs(z_score) > 2 then 'WARNING: Unusual price (>2 std dev)'
        when abs(variation_jour_pct) > 10 then 'WARNING: Large daily change (>10%)'
        else 'Normal'
    end as anomaly_status
from station_prices
where abs(z_score) > 2 or abs(variation_jour_pct) > 10
order by abs(z_score) desc, date_prix desc
limit 100
