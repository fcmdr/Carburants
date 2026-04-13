{% snapshot snap_stations %}

{{
    config(
        target_schema='snapshots',
        unique_key='station_id',
        strategy='check',
        check_cols=[
            'adresse',
            'ville',
            'code_postal',
            'services_list',
            'is_automate_24_24',
            'type_station'
        ],
        invalidate_hard_deletes=True
    )
}}

/*
    Snapshot: Stations (SCD Type 2)
    -------------------------------
    Tracks changes to station attributes over time.
    Uses check strategy to detect changes in key fields:
    - Address changes
    - Service additions/removals
    - Operating mode changes (24/24 automation)

    This enables historical analysis of station characteristics.
*/

select
    station_id,
    latitude,
    longitude,
    code_postal,
    code_departement,
    region,
    ville,
    adresse,
    type_station,
    is_automate_24_24,
    is_ouvert_7j_7,
    nb_services,
    services_list,
    services_categories,
    has_lavage,
    has_restauration,
    has_boutique,
    has_borne_electrique,
    has_gpl,
    data_quality_score,
    _loaded_at as source_loaded_at

from {{ ref('int_stations_enriched') }}

{% endsnapshot %}
