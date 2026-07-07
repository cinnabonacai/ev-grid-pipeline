{{ config(materialized='table') }}

select
    state,
    count(*)                    as station_count,
    sum(dc_fast_ports)          as total_dc_fast_ports,
    sum(level2_ports)           as total_level2_ports,
    count(distinct ev_network)  as network_count,
    max(_snapshot_date)         as as_of_date
from {{ ref('stg_ev_stations') }}
group by 1
