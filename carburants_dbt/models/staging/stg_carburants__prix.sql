{{
    config(
        materialized='view',
        tags=['staging', 'prix']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_prix') }}
),

cleaned as (
    select
        -- Composite key components
        station_id,
        trim(upper(carburant_nom)) as carburant_nom,
        trim(carburant_id) as carburant_id,

        -- Price is already in euros (e.g., 2.299)
        try_to_decimal(valeur, 10, 3) as prix_euros,

        -- Parse the update timestamp (format: YYYY-MM-DD HH24:MI:SS)
        try_to_timestamp(maj) as date_maj,

        -- Extract date for easier filtering
        date(try_to_timestamp(maj)) as date_prix,

        -- Data quality flags
        case
            when try_to_decimal(valeur, 10, 3) is null then true
            else false
        end as is_prix_invalide,

        case
            when try_to_decimal(valeur, 10, 3) < 0.5 then true
            when try_to_decimal(valeur, 10, 3) > 5.0 then true
            else false
        end as is_prix_suspect,

        -- Metadata
        _loaded_at

    from source
    where
        station_id is not null
        and carburant_nom is not null
        and valeur is not null
)

select * from cleaned
