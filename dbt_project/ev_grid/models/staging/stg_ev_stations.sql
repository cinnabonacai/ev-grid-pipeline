with source as (
    select * from {{ source('raw', 'NLR_EV_STATIONS_RAW') }}
)

select
    station_id,
    station_name,
    city,
    state,
    latitude,
    longitude,
    ev_network,
    coalesce(ev_dc_fast_num, 0)   as dc_fast_ports,
    coalesce(ev_level2_num, 0)    as level2_ports,
    try_cast(open_date as date)   as open_date,
    _snapshot_date
from source
where status_code = 'E'
