{{
    config(
        materialized='view',
        tags=['staging', 'horaires']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_stations') }}
),

-- Parse the horaires_json field: format is "Lundi:0:08.00-20.00|Mardi:0:08.00-20.00|..."
horaires_split as (
    select
        station_id,
        trim(value::string) as jour_raw,
        _loaded_at
    from source,
    lateral flatten(input => split(horaires_json, '|'))
    where horaires_json is not null and horaires_json != ''
),

parsed as (
    select
        station_id,

        -- Parse jour name (before first colon)
        trim(split_part(jour_raw, ':', 1)) as jour_nom,

        -- Parse ferme flag (second part)
        case
            when trim(split_part(jour_raw, ':', 2)) = '1' then true
            else false
        end as is_ferme,

        -- Parse horaires (third part onwards)
        trim(split_part(jour_raw, ':', 3)) as horaires_raw,

        _loaded_at
    from horaires_split
),

cleaned as (
    select
        station_id,

        -- Standardize day names
        case upper(jour_nom)
            when 'LUNDI' then 1
            when 'MARDI' then 2
            when 'MERCREDI' then 3
            when 'JEUDI' then 4
            when 'VENDREDI' then 5
            when 'SAMEDI' then 6
            when 'DIMANCHE' then 7
            else null
        end as jour_numero,

        initcap(jour_nom) as jour_nom,
        is_ferme,

        -- Parse opening hours (format: HH.MM-HH.MM;HH.MM-HH.MM)
        case
            when not is_ferme and horaires_raw != ''
                then replace(split_part(split_part(horaires_raw, ';', 1), '-', 1), '.', ':')
            else null
        end as ouverture_1,

        case
            when not is_ferme and horaires_raw != ''
                then replace(split_part(split_part(horaires_raw, ';', 1), '-', 2), '.', ':')
            else null
        end as fermeture_1,

        -- Second opening period (if exists, e.g., lunch break)
        case
            when not is_ferme and contains(horaires_raw, ';')
                then replace(split_part(split_part(horaires_raw, ';', 2), '-', 1), '.', ':')
            else null
        end as ouverture_2,

        case
            when not is_ferme and contains(horaires_raw, ';')
                then replace(split_part(split_part(horaires_raw, ';', 2), '-', 2), '.', ':')
            else null
        end as fermeture_2,

        horaires_raw,
        _loaded_at

    from parsed
    where jour_nom is not null and trim(jour_nom) != ''
)

select * from cleaned
