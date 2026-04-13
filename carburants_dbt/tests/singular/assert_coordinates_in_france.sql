/*
    Test: assert_coordinates_in_france
    ------------------------------------
    Verifies that station coordinates fall within France's bounding box.

    Metropolitan France approximate bounds:
    - Latitude: 41.3° to 51.1° N
    - Longitude: -5.1° to 9.6° E

    DOM-TOM are excluded from this check as they have different bounds.

    Returns stations with coordinates clearly outside France.
*/

with stations_metro as (
    select
        station_id,
        latitude,
        longitude,
        code_postal,
        ville,
        code_departement
    from {{ ref('stg_carburants__stations') }}
    where
        -- Exclude DOM-TOM (postal codes starting with 97x)
        code_departement not like '97%'
        and code_departement not like '98%'
        and latitude is not null
        and longitude is not null
),

out_of_bounds as (
    select
        station_id,
        latitude,
        longitude,
        code_postal,
        ville,
        case
            when latitude < 41.0 then 'Latitude too low (south of France)'
            when latitude > 52.0 then 'Latitude too high (north of France)'
            when longitude < -6.0 then 'Longitude too low (west of France)'
            when longitude > 10.0 then 'Longitude too high (east of France)'
            else 'Unknown issue'
        end as coordinate_issue
    from stations_metro
    where
        latitude < 41.0
        or latitude > 52.0
        or longitude < -6.0
        or longitude > 10.0
)

select *
from out_of_bounds
