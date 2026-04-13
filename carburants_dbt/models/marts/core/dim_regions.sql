{{
    config(
        materialized='table',
        tags=['core', 'dimensional', 'dimension']
    )
}}

with departements as (
    select distinct
        code_departement,
        region
    from {{ ref('int_stations_enriched') }}
    where code_departement is not null
),

-- French regions with metadata
regions_data as (
    select
        region,
        case region
            when 'Île-de-France' then 'IDF'
            when 'Grand Est' then 'GES'
            when 'Hauts-de-France' then 'HDF'
            when 'Normandie' then 'NOR'
            when 'Bretagne' then 'BRE'
            when 'Centre-Val de Loire' then 'CVL'
            when 'Pays de la Loire' then 'PDL'
            when 'Nouvelle-Aquitaine' then 'NAQ'
            when 'Occitanie' then 'OCC'
            when 'Auvergne-Rhône-Alpes' then 'ARA'
            when 'Provence-Alpes-Côte d''Azur' then 'PAC'
            when 'Corse' then 'COR'
            when 'Bourgogne-Franche-Comté' then 'BFC'
            when 'DOM-TOM' then 'DOM'
            else 'AUT'
        end as region_code,
        case region
            when 'Île-de-France' then 'Paris'
            when 'Grand Est' then 'Strasbourg'
            when 'Hauts-de-France' then 'Lille'
            when 'Normandie' then 'Rouen'
            when 'Bretagne' then 'Rennes'
            when 'Centre-Val de Loire' then 'Orléans'
            when 'Pays de la Loire' then 'Nantes'
            when 'Nouvelle-Aquitaine' then 'Bordeaux'
            when 'Occitanie' then 'Toulouse'
            when 'Auvergne-Rhône-Alpes' then 'Lyon'
            when 'Provence-Alpes-Côte d''Azur' then 'Marseille'
            when 'Corse' then 'Ajaccio'
            when 'Bourgogne-Franche-Comté' then 'Dijon'
            else null
        end as chef_lieu
    from (select distinct region from departements)
),

-- Count departments per region
dept_counts as (
    select
        region,
        count(distinct code_departement) as nb_departements,
        listagg(distinct code_departement, ', ') within group (order by code_departement) as departements_list
    from departements
    group by region
),

-- Count stations per region
station_counts as (
    select
        region,
        count(distinct station_id) as nb_stations
    from {{ ref('int_stations_enriched') }}
    group by region
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['r.region']) }} as region_key,

    -- Natural key
    r.region_code,

    -- Attributes
    r.region as region_nom,
    r.chef_lieu,
    coalesce(d.nb_departements, 0) as nb_departements,
    d.departements_list,
    coalesce(s.nb_stations, 0) as nb_stations,

    -- Metadata
    current_timestamp() as dbt_updated_at

from regions_data r
left join dept_counts d on r.region = d.region
left join station_counts s on r.region = s.region
