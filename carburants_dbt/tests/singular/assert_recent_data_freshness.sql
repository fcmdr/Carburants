/*
    Test: assert_recent_data_freshness
    -----------------------------------
    Verifies that we have received fresh data within the expected timeframe.

    This test checks:
    1. The latest ingestion was successful
    2. The most recent price data is not older than 24 hours
    3. We have a minimum number of stations with recent prices

    Failure indicates potential issues with:
    - The ingestion pipeline
    - The data source availability
    - Network/connectivity problems
*/

with latest_ingestion as (
    select
        max(completed_at) as last_successful_ingestion,
        max(case when status = 'SUCCESS' then completed_at end) as last_success
    from {{ source('raw', 'raw_ingestion_log') }}
),

latest_prices as (
    select
        max(date_maj) as most_recent_price_update,
        count(distinct station_id) as stations_with_recent_data
    from {{ ref('stg_carburants__prix') }}
    where date_maj >= dateadd(hour, -{{ var('freshness_threshold_hours', 24) }}, current_timestamp())
),

freshness_check as (
    select
        i.last_successful_ingestion,
        i.last_success,
        p.most_recent_price_update,
        p.stations_with_recent_data,

        -- Check conditions
        case
            when i.last_success is null then 'No successful ingestion found'
            when i.last_success < dateadd(hour, -{{ var('freshness_threshold_hours', 24) }}, current_timestamp())
                then 'Last successful ingestion is too old'
            when p.most_recent_price_update is null then 'No recent price data found'
            when p.most_recent_price_update < dateadd(hour, -{{ var('freshness_threshold_hours', 24) }}, current_timestamp())
                then 'Most recent price update is too old'
            when p.stations_with_recent_data < 100 then 'Too few stations with recent data'
            else 'OK'
        end as freshness_status

    from latest_ingestion i
    cross join latest_prices p
)

-- Return rows only if there's a problem (test fails if any rows returned)
select *
from freshness_check
where freshness_status != 'OK'
