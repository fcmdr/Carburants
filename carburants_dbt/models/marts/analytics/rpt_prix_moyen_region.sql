{{
    config(
        materialized='table',
        tags=['analytics', 'reporting']
    )
}}

/*
    Report: Prix Moyen par Région
    ----------------------------
    Aggregated fuel prices by region and fuel type.
    Used for regional price comparison dashboards.
*/

with fct_prix as (
    select * from {{ ref('fct_prix_carburants') }}
    where date_prix >= dateadd(day, -30, current_date())
),

dim_regions as (
    select * from {{ ref('dim_regions') }}
),

daily_regional_avg as (
    select
        region,
        carburant_nom,
        date_prix,
        avg(prix_euros) as prix_moyen,
        min(prix_euros) as prix_min,
        max(prix_euros) as prix_max,
        count(distinct station_id) as nb_stations,
        stddev(prix_euros) as prix_ecart_type
    from fct_prix
    group by region, carburant_nom, date_prix
),

latest_date as (
    select max(date_prix) as max_date from fct_prix
),

regional_stats as (
    select
        d.region,
        d.carburant_nom,
        d.date_prix,
        d.prix_moyen,
        d.prix_min,
        d.prix_max,
        d.nb_stations,
        d.prix_ecart_type,

        -- Price vs national average
        avg(d.prix_moyen) over (partition by d.carburant_nom, d.date_prix) as prix_moyen_national,

        -- Ranking
        row_number() over (
            partition by d.carburant_nom, d.date_prix
            order by d.prix_moyen asc
        ) as rang_region

    from daily_regional_avg d
),

final as (
    select
        r.region_key,
        s.region,
        s.carburant_nom,
        s.date_prix,

        -- Prices
        round(s.prix_moyen, 3) as prix_moyen_euros,
        round(s.prix_min, 3) as prix_min_euros,
        round(s.prix_max, 3) as prix_max_euros,
        round(s.prix_ecart_type, 4) as prix_ecart_type,

        -- Comparison to national
        round(s.prix_moyen_national, 3) as prix_moyen_national,
        round(s.prix_moyen - s.prix_moyen_national, 3) as ecart_vs_national,
        round((s.prix_moyen - s.prix_moyen_national) / s.prix_moyen_national * 100, 2) as ecart_vs_national_pct,

        -- Classification
        case
            when s.prix_moyen < s.prix_moyen_national * 0.98 then 'Moins cher que la moyenne'
            when s.prix_moyen > s.prix_moyen_national * 1.02 then 'Plus cher que la moyenne'
            else 'Dans la moyenne'
        end as comparaison_nationale,

        -- Stats
        s.nb_stations,
        s.rang_region,
        (select count(distinct region) from regional_stats where carburant_nom = s.carburant_nom and date_prix = s.date_prix) as total_regions,

        -- Latest flag
        case when s.date_prix = (select max_date from latest_date) then true else false end as is_latest,

        current_timestamp() as dbt_updated_at

    from regional_stats s
    left join dim_regions r on s.region = r.region_nom
)

select * from final
