{{
    config(
        materialized='table',
        tags=['analytics', 'reporting']
    )
}}

/*
    Report: Évolution des Prix Hebdomadaire
    ---------------------------------------
    Weekly price evolution analysis by fuel type and region.
    Used for trend analysis and price forecasting.
*/

with fct_prix as (
    select * from {{ ref('fct_prix_carburants') }}
    where date_prix >= dateadd(week, -12, current_date())  -- Last 12 weeks
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

-- Aggregate to weekly level
weekly_national as (
    select
        d.annee,
        d.semaine_annee as semaine,
        d.debut_semaine,
        d.fin_semaine,
        f.carburant_nom,

        avg(f.prix_euros) as prix_moyen,
        min(f.prix_euros) as prix_min,
        max(f.prix_euros) as prix_max,
        percentile_cont(0.5) within group (order by f.prix_euros) as prix_median,
        stddev(f.prix_euros) as prix_ecart_type,
        count(distinct f.station_id) as nb_stations,
        count(*) as nb_observations

    from fct_prix f
    join dim_date d on f.date_prix = d.date_day
    group by d.annee, d.semaine_annee, d.debut_semaine, d.fin_semaine, f.carburant_nom
),

weekly_regional as (
    select
        d.annee,
        d.semaine_annee as semaine,
        d.debut_semaine,
        f.region,
        f.carburant_nom,

        avg(f.prix_euros) as prix_moyen_region

    from fct_prix f
    join dim_date d on f.date_prix = d.date_day
    group by d.annee, d.semaine_annee, d.debut_semaine, f.region, f.carburant_nom
),

with_variations as (
    select
        w.*,

        -- Previous week
        lag(w.prix_moyen) over (
            partition by w.carburant_nom
            order by w.debut_semaine
        ) as prix_semaine_precedente,

        -- 4 weeks ago
        lag(w.prix_moyen, 4) over (
            partition by w.carburant_nom
            order by w.debut_semaine
        ) as prix_mois_precedent,

        -- Same week last year (52 weeks ago)
        lag(w.prix_moyen, 52) over (
            partition by w.carburant_nom
            order by w.debut_semaine
        ) as prix_annee_precedente

    from weekly_national w
),

-- Pivot regional data for the top regions
regional_pivot as (
    select
        annee,
        semaine,
        debut_semaine,
        carburant_nom,
        max(case when region = 'Île-de-France' then prix_moyen_region end) as prix_idf,
        max(case when region = 'Provence-Alpes-Côte d''Azur' then prix_moyen_region end) as prix_paca,
        max(case when region = 'Auvergne-Rhône-Alpes' then prix_moyen_region end) as prix_ara,
        max(case when region = 'Nouvelle-Aquitaine' then prix_moyen_region end) as prix_naq,
        max(case when region = 'Occitanie' then prix_moyen_region end) as prix_occ
    from weekly_regional
    group by annee, semaine, debut_semaine, carburant_nom
),

final as (
    select
        -- Time dimensions
        v.annee,
        v.semaine,
        v.debut_semaine,
        v.fin_semaine,
        v.carburant_nom,

        -- National statistics
        round(v.prix_moyen, 3) as prix_moyen_national,
        round(v.prix_min, 3) as prix_min_national,
        round(v.prix_max, 3) as prix_max_national,
        round(v.prix_median, 3) as prix_median_national,
        round(v.prix_ecart_type, 4) as prix_ecart_type,
        v.nb_stations,
        v.nb_observations,

        -- Week-over-week variation
        round(v.prix_semaine_precedente, 3) as prix_semaine_precedente,
        round(v.prix_moyen - v.prix_semaine_precedente, 3) as variation_hebdo,
        round((v.prix_moyen - v.prix_semaine_precedente) / nullif(v.prix_semaine_precedente, 0) * 100, 2) as variation_hebdo_pct,

        -- Month-over-month variation (4 weeks)
        round(v.prix_mois_precedent, 3) as prix_mois_precedent,
        round(v.prix_moyen - v.prix_mois_precedent, 3) as variation_mensuelle,
        round((v.prix_moyen - v.prix_mois_precedent) / nullif(v.prix_mois_precedent, 0) * 100, 2) as variation_mensuelle_pct,

        -- Year-over-year variation
        round(v.prix_annee_precedente, 3) as prix_annee_precedente,
        round(v.prix_moyen - v.prix_annee_precedente, 3) as variation_annuelle,
        round((v.prix_moyen - v.prix_annee_precedente) / nullif(v.prix_annee_precedente, 0) * 100, 2) as variation_annuelle_pct,

        -- Trend classification
        case
            when (v.prix_moyen - v.prix_semaine_precedente) / nullif(v.prix_semaine_precedente, 0) * 100 > 3 then 'Forte hausse'
            when (v.prix_moyen - v.prix_semaine_precedente) / nullif(v.prix_semaine_precedente, 0) * 100 > 1 then 'Hausse modérée'
            when (v.prix_moyen - v.prix_semaine_precedente) / nullif(v.prix_semaine_precedente, 0) * 100 < -3 then 'Forte baisse'
            when (v.prix_moyen - v.prix_semaine_precedente) / nullif(v.prix_semaine_precedente, 0) * 100 < -1 then 'Baisse modérée'
            else 'Stable'
        end as tendance_hebdo,

        -- Regional breakdown (top regions)
        round(r.prix_idf, 3) as prix_ile_de_france,
        round(r.prix_paca, 3) as prix_paca,
        round(r.prix_ara, 3) as prix_auvergne_rhone_alpes,
        round(r.prix_naq, 3) as prix_nouvelle_aquitaine,
        round(r.prix_occ, 3) as prix_occitanie,

        -- Regional spread
        round(greatest(
            coalesce(r.prix_idf, 0),
            coalesce(r.prix_paca, 0),
            coalesce(r.prix_ara, 0),
            coalesce(r.prix_naq, 0),
            coalesce(r.prix_occ, 0)
        ) - least(
            coalesce(r.prix_idf, 999),
            coalesce(r.prix_paca, 999),
            coalesce(r.prix_ara, 999),
            coalesce(r.prix_naq, 999),
            coalesce(r.prix_occ, 999)
        ), 3) as ecart_regional_max,

        current_timestamp() as dbt_updated_at

    from with_variations v
    left join regional_pivot r
        on v.debut_semaine = r.debut_semaine
        and v.carburant_nom = r.carburant_nom
)

select * from final
order by carburant_nom, debut_semaine desc
