{{
    config(
        materialized='view',
        tags=['staging', 'services']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_stations') }}
),

-- Unpivot services from comma-separated string to rows
services_split as (
    select
        station_id,
        trim(value::string) as service_nom,
        _loaded_at
    from source,
    lateral flatten(input => split(services, ','))
    where services is not null and services != ''
),

cleaned as (
    select
        station_id,
        upper(service_nom) as service_nom,

        -- Categorize services
        case
            when upper(service_nom) like '%LAVAGE%' then 'Lavage'
            when upper(service_nom) like '%GONFLAGE%' then 'Entretien'
            when upper(service_nom) like '%VIDANGE%' then 'Entretien'
            when upper(service_nom) like '%BOUTIQUE%' then 'Commerce'
            when upper(service_nom) like '%RESTAURATION%' then 'Restauration'
            when upper(service_nom) like '%RELAIS%' then 'Commerce'
            when upper(service_nom) like '%TOILETTES%' then 'Commodités'
            when upper(service_nom) like '%WC%' then 'Commodités'
            when upper(service_nom) like '%DOUCHES%' then 'Commodités'
            when upper(service_nom) like '%WIFI%' then 'Commodités'
            when upper(service_nom) like '%BORNE%' then 'Recharge'
            when upper(service_nom) like '%ELECTRIQUE%' then 'Recharge'
            when upper(service_nom) like '%GPL%' then 'Carburant'
            when upper(service_nom) like '%GNV%' then 'Carburant'
            when upper(service_nom) like '%AUTOMATE%' then 'Paiement'
            when upper(service_nom) like '%CB%' then 'Paiement'
            when upper(service_nom) like '%DAB%' then 'Paiement'
            when upper(service_nom) like '%PISTE%' then 'Entretien'
            else 'Autre'
        end as service_categorie,

        _loaded_at

    from services_split
    where service_nom is not null and trim(service_nom) != ''
)

select distinct
    station_id,
    service_nom,
    service_categorie,
    _loaded_at
from cleaned
