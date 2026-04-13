{{
    config(
        materialized='table',
        tags=['core', 'dimensional', 'dimension']
    )
}}

-- Generate a date spine from 2023 to 2030
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

date_base as (
    select
        cast(date_day as date) as date_day
    from date_spine
),

date_enriched as (
    select
        date_day,

        -- Year
        extract(year from date_day) as year_number,
        date_trunc('year', date_day) as year_start_date,
        last_day(date_day, 'year') as year_end_date,

        -- Quarter
        extract(quarter from date_day) as quarter_of_year,
        date_trunc('quarter', date_day) as quarter_start_date,
        last_day(date_day, 'quarter') as quarter_end_date,

        -- Month
        extract(month from date_day) as month_of_year,
        to_char(date_day, 'MMMM') as month_name,
        date_trunc('month', date_day) as month_start_date,
        last_day(date_day, 'month') as month_end_date,

        -- Week
        extract(week from date_day) as week_of_year,
        date_trunc('week', date_day) as week_start_date,
        dateadd('day', 6, date_trunc('week', date_day)) as week_end_date,
        extract(weekiso from date_day) as iso_week_of_year,

        -- Day
        extract(dayofweek from date_day) as day_of_week_snowflake, -- 0=Sunday in Snowflake
        to_char(date_day, 'DY') as day_name,
        extract(day from date_day) as day_of_month,
        extract(dayofyear from date_day) as day_of_year,

        -- Prior periods
        dateadd('year', -1, date_day) as prior_year_date_day

    from date_base
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,

    -- Natural key
    date_day,

    -- Year
    year_number as annee,
    year_start_date as debut_annee,
    year_end_date as fin_annee,

    -- Quarter
    quarter_of_year as trimestre,
    quarter_start_date as debut_trimestre,
    quarter_end_date as fin_trimestre,

    -- Month
    month_of_year as mois,
    month_name as mois_nom_en,
    case month_of_year
        when 1 then 'Janvier'
        when 2 then 'Février'
        when 3 then 'Mars'
        when 4 then 'Avril'
        when 5 then 'Mai'
        when 6 then 'Juin'
        when 7 then 'Juillet'
        when 8 then 'Août'
        when 9 then 'Septembre'
        when 10 then 'Octobre'
        when 11 then 'Novembre'
        when 12 then 'Décembre'
    end as mois_nom_fr,
    month_start_date as debut_mois,
    month_end_date as fin_mois,

    -- Week
    week_of_year as semaine_annee,
    week_start_date as debut_semaine,
    week_end_date as fin_semaine,
    iso_week_of_year as semaine_iso,

    -- Day (convert Snowflake 0=Sunday to ISO 1=Monday)
    case when day_of_week_snowflake = 0 then 7 else day_of_week_snowflake end as jour_semaine,
    day_name as jour_nom_en,
    case day_of_week_snowflake
        when 1 then 'Lundi'
        when 2 then 'Mardi'
        when 3 then 'Mercredi'
        when 4 then 'Jeudi'
        when 5 then 'Vendredi'
        when 6 then 'Samedi'
        when 0 then 'Dimanche'
    end as jour_nom_fr,
    day_of_month as jour_mois,
    day_of_year as jour_annee,

    -- Flags
    day_of_week_snowflake in (0, 6) as is_weekend,
    day_of_week_snowflake not in (0, 6) as is_weekday,

    -- French holidays (simplified - main ones)
    case
        when month_of_year = 1 and day_of_month = 1 then true  -- Jour de l'An
        when month_of_year = 5 and day_of_month = 1 then true  -- Fête du Travail
        when month_of_year = 5 and day_of_month = 8 then true  -- Victoire 1945
        when month_of_year = 7 and day_of_month = 14 then true -- Fête Nationale
        when month_of_year = 8 and day_of_month = 15 then true -- Assomption
        when month_of_year = 11 and day_of_month = 1 then true -- Toussaint
        when month_of_year = 11 and day_of_month = 11 then true -- Armistice
        when month_of_year = 12 and day_of_month = 25 then true -- Noël
        else false
    end as is_jour_ferie,

    -- Prior periods
    prior_year_date_day,

    -- Metadata
    current_timestamp() as dbt_updated_at

from date_enriched
