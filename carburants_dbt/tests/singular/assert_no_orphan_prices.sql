/*
    Test: assert_no_orphan_prices
    ------------------------------
    Verifies that all price records have a corresponding station.

    Orphan prices (prices without matching stations) indicate:
    - Data integrity issues in the source
    - Ingestion timing problems
    - Station data missing from the feed

    This test should pass with 0 rows returned.
*/

with prices as (
    select distinct station_id
    from {{ ref('stg_carburants__prix') }}
),

stations as (
    select distinct station_id
    from {{ ref('stg_carburants__stations') }}
),

orphan_prices as (
    select
        p.station_id,
        count(*) as price_records
    from prices p
    left join stations s on p.station_id = s.station_id
    where s.station_id is null
    group by p.station_id
)

select *
from orphan_prices
