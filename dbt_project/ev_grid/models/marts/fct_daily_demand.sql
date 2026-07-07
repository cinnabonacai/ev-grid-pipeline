{{
    config(
        materialized='incremental',
        unique_key=['demand_date', 'balancing_authority'],
        incremental_strategy='delete+insert'
    )
}}

select
    demand_date,
    balancing_authority,
    balancing_authority_name,
    sum(demand_mwh)                as total_demand_mwh,
    avg(demand_mwh)                as avg_hourly_demand_mwh,
    max(demand_mwh)                as peak_hourly_demand_mwh,
    min(demand_mwh)                as min_hourly_demand_mwh,
    count(*)                       as hours_reported
from {{ ref('stg_eia_demand') }}
where demand_mwh is not null

{% if is_incremental() %}
  -- only reprocess the last 3 days on scheduled runs (late-arriving data buffer)
  and demand_date >= (select dateadd(day, -3, max(demand_date)) from {{ this }})
{% endif %}

group by 1, 2, 3
