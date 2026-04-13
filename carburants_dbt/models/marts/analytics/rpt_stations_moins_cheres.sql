{{
    config(
        materialized='table',
        tags=['analytics', 'reporting']
    )
}}

/*
    Report: Stations les Moins Chères
    ---------------------------------
    Top cheapest stations per region and fuel type.
    Updated daily for consumer-facing applications.
*/

with fct_prix as (
    select * from {{ ref('fct_prix_carburants') }}
    where date_prix = (select max(date_prix) from {{ ref('fct_prix_carburants') }})
),

dim_stations as (
    select * from {{ ref('dim_stations') }}
),

ranked_stations as (
    select
        f.station_id,
        f.carburant_nom,
        f.region,
        f.code_departement,
        f.prix_euros,
        f.rang_national,
        f.rang_region,
        f.rang_departement,
        f.categorie_prix,
        f.tendance_prix,
        f.date_prix,

        -- Get top 10 per region per fuel type
        row_number() over (
            partition by f.region, f.carburant_nom
            order by f.prix_euros asc
        ) as rang_dans_region,

        -- Get top 100 national per fuel type
        row_number() over (
            partition by f.carburant_nom
            order by f.prix_euros asc
        ) as rang_dans_france

    from fct_prix f
),

top_stations as (
    select *
    from ranked_stations
    where rang_dans_region <= 10 or rang_dans_france <= 100
),

final as (
    select
        -- Station info
        s.station_key,
        t.station_id,
        s.ville,
        s.adresse,
        s.code_postal,
        t.code_departement,
        t.region,

        -- Station attributes
        s.type_station,
        s.is_automate_24_24,
        s.is_ouvert_7j_7,
        s.nb_services,
        s.services_list,
        s.has_lavage,
        s.has_restauration,
        s.has_boutique,

        -- Fuel info
        t.carburant_nom,
        t.prix_euros,
        t.categorie_prix,
        t.tendance_prix,

        -- Rankings
        t.rang_national,
        t.rang_region,
        t.rang_departement,
        t.rang_dans_region as position_regionale_top10,
        t.rang_dans_france as position_nationale_top100,

        -- Flags
        t.rang_dans_france <= 10 as is_top10_france,
        t.rang_dans_france <= 100 as is_top100_france,
        t.rang_dans_region <= 3 as is_top3_region,
        t.rang_dans_region <= 10 as is_top10_region,

        -- Location for mapping
        s.latitude,
        s.longitude,

        -- Date
        t.date_prix,

        current_timestamp() as dbt_updated_at

    from top_stations t
    left join dim_stations s on t.station_id = s.station_id
)

select * from final
order by carburant_nom, rang_national
