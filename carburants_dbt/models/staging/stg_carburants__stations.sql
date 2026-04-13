{{
    config(
        materialized='view',
        tags=['staging', 'stations']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_stations') }}
),

cleaned as (
    select
        -- Primary key
        station_id,

        -- Geographic coordinates (raw values are multiplied by 100000)
        {{ clean_coordinates('latitude') }} as latitude,
        {{ clean_coordinates('longitude') }} as longitude,

        -- Location information
        trim(upper(cp)) as code_postal,
        left(trim(upper(cp)), 2) as code_departement,
        case
            when left(trim(upper(cp)), 2) in ('97', '98') then left(trim(upper(cp)), 3)
            else left(trim(upper(cp)), 2)
        end as code_departement_extended,
        trim(upper(ville)) as ville,
        trim(adresse) as adresse,

        -- Station type
        case
            when upper(pop) = 'R' then 'Route'
            when upper(pop) = 'A' then 'Autoroute'
            else 'Inconnu'
        end as type_station,

        -- 24/24 automation
        case
            when lower(automate_24_24) = 'oui' then true
            when lower(automate_24_24) = '1' then true
            else false
        end as is_automate_24_24,

        -- Raw services and horaires for downstream processing
        services as services_raw,
        horaires_json as horaires_raw,

        -- Metadata
        _loaded_at

    from source
    where station_id is not null
)

select * from cleaned
