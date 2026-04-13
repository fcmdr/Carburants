{% snapshot snap_prix %}

{{
    config(
        target_schema='snapshots',
        unique_key='prix_snapshot_key',
        strategy='timestamp',
        updated_at='date_maj',
        invalidate_hard_deletes=False
    )
}}

/*
    Snapshot: Prix (SCD Type 2)
    ---------------------------
    Tracks fuel price changes over time using timestamp strategy.
    Each price update creates a new snapshot record.

    This enables:
    - Historical price analysis
    - Price trend visualization
    - Station price volatility analysis
*/

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'carburant_nom']) }} as prix_snapshot_key,
    station_id,
    carburant_nom,
    carburant_id,
    prix_euros,
    date_maj,
    date_prix,
    is_prix_invalide,
    is_prix_suspect,
    _loaded_at as source_loaded_at

from {{ ref('stg_carburants__prix') }}
where not is_prix_invalide

{% endsnapshot %}
