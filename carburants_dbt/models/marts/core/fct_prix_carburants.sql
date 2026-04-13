{{
    config(
        materialized='table',
        tags=['core', 'dimensional', 'fact'],
        cluster_by=['date_prix', 'carburant_nom']
    )
}}

with prix as (
    select * from {{ ref('int_prix_daily_agg') }}
),

dim_stations as (
    select station_key, station_id from {{ ref('dim_stations') }}
),

dim_carburants as (
    select carburant_key, carburant_id from {{ ref('dim_carburants') }}
),

dim_date as (
    select date_key, date_day from {{ ref('dim_date') }}
),

dim_regions as (
    select region_key, region_nom from {{ ref('dim_regions') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.station_id', 'p.carburant_nom', 'p.date_prix']) }} as prix_key,

        -- Foreign keys (surrogate)
        ds.station_key,
        dc.carburant_key,
        dd.date_key,
        dr.region_key,

        -- Natural keys (for convenience)
        p.station_id,
        p.carburant_nom,
        p.carburant_id,
        p.date_prix,

        -- Location attributes (denormalized for performance)
        p.code_departement,
        p.region,
        p.type_station,

        -- Measures
        p.prix_euros,
        p.nb_updates_jour,

        -- Variations
        p.prix_veille,
        p.variation_jour,
        p.variation_jour_pct,
        p.prix_semaine_precedente,
        p.variation_semaine,
        p.variation_semaine_pct,
        p.prix_mois_precedent,
        p.variation_mois,
        p.variation_mois_pct,

        -- Rankings
        p.rang_national,
        p.rang_departement,
        p.rang_region,
        p.percentile_national,

        -- Categorization based on percentile
        case
            when p.percentile_national <= 0.1 then 'Très bon marché'
            when p.percentile_national <= 0.25 then 'Bon marché'
            when p.percentile_national <= 0.75 then 'Prix moyen'
            when p.percentile_national <= 0.9 then 'Cher'
            else 'Très cher'
        end as categorie_prix,

        -- Price trend indicator
        case
            when p.variation_semaine_pct > 2 then 'Forte hausse'
            when p.variation_semaine_pct > 0.5 then 'Hausse'
            when p.variation_semaine_pct < -2 then 'Forte baisse'
            when p.variation_semaine_pct < -0.5 then 'Baisse'
            else 'Stable'
        end as tendance_prix,

        -- Metadata
        p.derniere_maj,
        current_timestamp() as dbt_updated_at

    from prix p
    left join dim_stations ds on p.station_id = ds.station_id
    left join dim_carburants dc on p.carburant_id = dc.carburant_id
    left join dim_date dd on p.date_prix = dd.date_day
    left join dim_regions dr on p.region = dr.region_nom
)

select * from final
