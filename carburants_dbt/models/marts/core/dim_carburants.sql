{{
    config(
        materialized='table',
        tags=['core', 'dimensional', 'dimension']
    )
}}

with seed_carburants as (
    select * from {{ ref('types_carburants') }}
),

prix_carburants as (
    select distinct
        carburant_nom,
        carburant_id
    from {{ ref('stg_carburants__prix') }}
),

-- Combine seed data with actual data from prices
combined as (
    select
        coalesce(s.carburant_id, p.carburant_id) as carburant_id,
        coalesce(s.carburant_nom, p.carburant_nom) as carburant_nom,
        s.carburant_nom_complet,
        s.categorie,
        s.est_essence,
        s.est_diesel,
        s.est_bio,
        s.taux_bio_pct,
        s.description
    from prix_carburants p
    left join seed_carburants s
        on upper(trim(p.carburant_nom)) = upper(trim(s.carburant_nom))
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['carburant_id']) }} as carburant_key,

    -- Natural key
    carburant_id,

    -- Attributes
    carburant_nom,
    coalesce(carburant_nom_complet, carburant_nom) as carburant_nom_complet,
    coalesce(categorie, 'Inconnu') as categorie,
    coalesce(est_essence, false) as est_essence,
    coalesce(est_diesel, false) as est_diesel,
    coalesce(est_bio, false) as est_bio,
    coalesce(taux_bio_pct, 0) as taux_bio_pct,
    description,

    -- Metadata
    current_timestamp() as dbt_updated_at

from combined
