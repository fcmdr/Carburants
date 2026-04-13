{{
    config(
        materialized='ephemeral',
        tags=['intermediate']
    )
}}

with stations as (
    select * from {{ ref('stg_carburants__stations') }}
),

services_agg as (
    select
        station_id,
        count(*) as nb_services,
        listagg(distinct service_nom, ', ') within group (order by service_nom) as services_list,
        listagg(distinct service_categorie, ', ') within group (order by service_categorie) as services_categories,
        max(case when service_categorie = 'Lavage' then 1 else 0 end) as has_lavage,
        max(case when service_categorie = 'Restauration' then 1 else 0 end) as has_restauration,
        max(case when service_categorie = 'Commerce' then 1 else 0 end) as has_boutique,
        max(case when service_categorie = 'Recharge' then 1 else 0 end) as has_borne_electrique,
        max(case when service_nom like '%GPL%' then 1 else 0 end) as has_gpl
    from {{ ref('stg_carburants__services') }}
    group by station_id
),

horaires_agg as (
    select
        station_id,
        count(case when not is_ferme then 1 end) as jours_ouverts,
        min(case when not is_ferme then ouverture_1 end) as heure_ouverture_min,
        max(case when not is_ferme then fermeture_1 end) as heure_fermeture_max
    from {{ ref('stg_carburants__horaires') }}
    group by station_id
),

prix_latest as (
    select
        station_id,
        count(distinct carburant_nom) as nb_carburants_disponibles,
        max(date_maj) as derniere_maj_prix,
        avg(prix_euros) as prix_moyen_tous_carburants
    from {{ ref('stg_carburants__prix') }}
    where not is_prix_invalide and not is_prix_suspect
    group by station_id
),

ruptures_en_cours as (
    select
        station_id,
        count(*) as nb_ruptures_en_cours,
        listagg(distinct carburant_nom, ', ') within group (order by carburant_nom) as carburants_en_rupture
    from {{ ref('stg_carburants__ruptures') }}
    where is_en_cours
    group by station_id
),

-- Region mapping based on department
region_mapping as (
    select
        station_id,
        code_departement,
        case
            when code_departement in ('75', '77', '78', '91', '92', '93', '94', '95') then 'Île-de-France'
            when code_departement in ('08', '10', '51', '52', '54', '55', '57', '67', '68', '88') then 'Grand Est'
            when code_departement in ('02', '59', '60', '62', '80') then 'Hauts-de-France'
            when code_departement in ('14', '27', '50', '61', '76') then 'Normandie'
            when code_departement in ('22', '29', '35', '56') then 'Bretagne'
            when code_departement in ('18', '28', '36', '37', '41', '45') then 'Centre-Val de Loire'
            when code_departement in ('44', '49', '53', '72', '85') then 'Pays de la Loire'
            when code_departement in ('16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87') then 'Nouvelle-Aquitaine'
            when code_departement in ('09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82') then 'Occitanie'
            when code_departement in ('01', '03', '07', '15', '26', '38', '42', '43', '63', '69', '73', '74') then 'Auvergne-Rhône-Alpes'
            when code_departement in ('04', '05', '06', '13', '83', '84') then 'Provence-Alpes-Côte d''Azur'
            when code_departement in ('2A', '2B', '20') then 'Corse'
            when code_departement in ('21', '25', '39', '58', '70', '71', '89', '90') then 'Bourgogne-Franche-Comté'
            when code_departement like '97%' then 'DOM-TOM'
            else 'Autre'
        end as region
    from stations
)

select
    s.station_id,
    s.latitude,
    s.longitude,
    s.code_postal,
    s.code_departement,
    r.region,
    s.ville,
    s.adresse,
    s.type_station,
    s.is_automate_24_24,

    -- Services
    coalesce(sv.nb_services, 0) as nb_services,
    sv.services_list,
    sv.services_categories,
    coalesce(sv.has_lavage, 0)::boolean as has_lavage,
    coalesce(sv.has_restauration, 0)::boolean as has_restauration,
    coalesce(sv.has_boutique, 0)::boolean as has_boutique,
    coalesce(sv.has_borne_electrique, 0)::boolean as has_borne_electrique,
    coalesce(sv.has_gpl, 0)::boolean as has_gpl,

    -- Horaires
    coalesce(h.jours_ouverts, 0) as jours_ouverts_semaine,
    h.heure_ouverture_min,
    h.heure_fermeture_max,
    case
        when s.is_automate_24_24 then true
        when h.jours_ouverts = 7 then true
        else false
    end as is_ouvert_7j_7,

    -- Prix info
    coalesce(p.nb_carburants_disponibles, 0) as nb_carburants_disponibles,
    p.derniere_maj_prix,
    p.prix_moyen_tous_carburants,

    -- Ruptures
    coalesce(rp.nb_ruptures_en_cours, 0) as nb_ruptures_en_cours,
    rp.carburants_en_rupture,

    -- Quality score (0-100)
    (
        case when s.latitude is not null and s.longitude is not null then 20 else 0 end +
        case when s.adresse is not null and s.adresse != '' then 10 else 0 end +
        case when coalesce(sv.nb_services, 0) > 0 then 20 else 0 end +
        case when coalesce(p.nb_carburants_disponibles, 0) >= 3 then 20 else 10 end +
        case when p.derniere_maj_prix >= dateadd(day, -1, current_timestamp()) then 30 else 10 end
    ) as data_quality_score,

    s._loaded_at

from stations s
left join services_agg sv on s.station_id = sv.station_id
left join horaires_agg h on s.station_id = h.station_id
left join prix_latest p on s.station_id = p.station_id
left join ruptures_en_cours rp on s.station_id = rp.station_id
left join region_mapping r on s.station_id = r.station_id
