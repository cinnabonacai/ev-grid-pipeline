{{ config(materialized='table') }}

with demand_30d as (
    select
        balancing_authority,
        avg(total_demand_mwh)  as avg_daily_demand_mwh,
        max(peak_hourly_demand_mwh) as peak_hourly_demand_mwh
    from {{ ref('fct_daily_demand') }}
    where demand_date >= dateadd(day, -30, current_date())
    group by 1
)

select
    m.state,
    m.ba_region_name,
    d.avg_daily_demand_mwh,
    d.peak_hourly_demand_mwh,
    e.station_count,
    e.total_dc_fast_ports,
    round(e.total_dc_fast_ports / nullif(d.avg_daily_demand_mwh, 0) * 1000, 2)
        as dc_fast_ports_per_gwh_daily_demand
from {{ ref('ba_state_map') }} m
join demand_30d d using (balancing_authority)
join {{ ref('dim_ev_infrastructure_by_state') }} e using (state)
