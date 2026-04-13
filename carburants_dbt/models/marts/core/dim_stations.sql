{{
    config(
        materialized='table',
        tags=['core', 'dimensional', 'dimension']
    )
}}

with stations as (
    select * from {{ ref('int_stations_enriched') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['station_id']) }} as station_key,

        -- Natural key
        station_id,

        -- Location
        latitude,
        longitude,
        code_postal,
        code_departement,
        region,
        ville,
        adresse,

        -- Attributes
        type_station,
        is_automate_24_24,
        is_ouvert_7j_7,

        -- Services
        nb_services,
        services_list,
        services_categories,
        has_lavage,
        has_restauration,
        has_boutique,
        has_borne_electrique,
        has_gpl,

        -- Operating hours
        jours_ouverts_semaine,
        heure_ouverture_min,
        heure_fermeture_max,

        -- Current state
        nb_carburants_disponibles,
        derniere_maj_prix,
        nb_ruptures_en_cours,
        carburants_en_rupture,

        -- Quality
        data_quality_score,

        -- Metadata
        _loaded_at as loaded_at,
        current_timestamp() as dbt_updated_at

    from stations
)

select * from final
