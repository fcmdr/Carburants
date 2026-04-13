{{
    config(
        materialized='ephemeral',
        tags=['intermediate']
    )
}}

with prix as (
    select * from {{ ref('stg_carburants__prix') }}
    where not is_prix_invalide
      and not is_prix_suspect
      and date_prix is not null
),

stations as (
    select * from {{ ref('int_stations_enriched') }}
),

-- Daily price aggregation per station and fuel type
daily_prix as (
    select
        p.station_id,
        p.carburant_nom,
        p.carburant_id,
        p.date_prix,

        -- Use the latest price of the day
        last_value(p.prix_euros) over (
            partition by p.station_id, p.carburant_nom, p.date_prix
            order by p.date_maj
            rows between unbounded preceding and unbounded following
        ) as prix_euros,

        -- Keep track of updates
        max(p.date_maj) over (
            partition by p.station_id, p.carburant_nom, p.date_prix
        ) as derniere_maj,
        count(*) over (
            partition by p.station_id, p.carburant_nom, p.date_prix
        ) as nb_updates_jour

    from prix p
),

-- Deduplicate to one row per station/fuel/date
daily_prix_dedup as (
    select distinct
        station_id,
        carburant_nom,
        carburant_id,
        date_prix,
        prix_euros,
        derniere_maj,
        nb_updates_jour
    from daily_prix
),

-- Add previous day price for variation calculation
with_variations as (
    select
        d.*,
        s.code_departement,
        s.region,
        s.type_station,

        -- Previous day price
        lag(d.prix_euros) over (
            partition by d.station_id, d.carburant_nom
            order by d.date_prix
        ) as prix_veille,

        -- Price 7 days ago
        lag(d.prix_euros, 7) over (
            partition by d.station_id, d.carburant_nom
            order by d.date_prix
        ) as prix_semaine_precedente,

        -- Price 30 days ago
        lag(d.prix_euros, 30) over (
            partition by d.station_id, d.carburant_nom
            order by d.date_prix
        ) as prix_mois_precedent

    from daily_prix_dedup d
    left join stations s on d.station_id = s.station_id
),

-- Calculate variations and rankings
final as (
    select
        station_id,
        carburant_nom,
        carburant_id,
        code_departement,
        region,
        type_station,
        date_prix,
        prix_euros,
        derniere_maj,
        nb_updates_jour,

        -- Variations
        prix_veille,
        prix_euros - prix_veille as variation_jour,
        case
            when prix_veille > 0 then round((prix_euros - prix_veille) / prix_veille * 100, 2)
            else null
        end as variation_jour_pct,

        prix_semaine_precedente,
        prix_euros - prix_semaine_precedente as variation_semaine,
        case
            when prix_semaine_precedente > 0 then round((prix_euros - prix_semaine_precedente) / prix_semaine_precedente * 100, 2)
            else null
        end as variation_semaine_pct,

        prix_mois_precedent,
        prix_euros - prix_mois_precedent as variation_mois,
        case
            when prix_mois_precedent > 0 then round((prix_euros - prix_mois_precedent) / prix_mois_precedent * 100, 2)
            else null
        end as variation_mois_pct,

        -- Rankings
        row_number() over (
            partition by carburant_nom, date_prix
            order by prix_euros asc
        ) as rang_national,

        row_number() over (
            partition by carburant_nom, date_prix, code_departement
            order by prix_euros asc
        ) as rang_departement,

        row_number() over (
            partition by carburant_nom, date_prix, region
            order by prix_euros asc
        ) as rang_region,

        -- Percentiles
        percent_rank() over (
            partition by carburant_nom, date_prix
            order by prix_euros
        ) as percentile_national

    from with_variations
)

select * from final
