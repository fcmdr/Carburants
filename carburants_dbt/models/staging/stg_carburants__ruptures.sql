{{
    config(
        materialized='view',
        tags=['staging', 'ruptures']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_ruptures') }}
),

cleaned as (
    select
        -- Keys
        station_id,
        trim(upper(carburant_nom)) as carburant_nom,
        trim(carburant_id) as carburant_id,

        -- Dates
        case
            when debut is not null and debut != ''
                then try_to_timestamp(debut, 'YYYY-MM-DD HH24:MI:SS')
            else null
        end as date_debut,

        case
            when fin is not null and fin != ''
                then try_to_timestamp(fin, 'YYYY-MM-DD HH24:MI:SS')
            else null
        end as date_fin,

        -- Rupture type
        trim(upper(type)) as type_rupture,

        -- Computed fields
        case
            when fin is null or fin = '' then true
            else false
        end as is_en_cours,

        -- Metadata
        _loaded_at

    from source
    where station_id is not null
)

select * from cleaned
