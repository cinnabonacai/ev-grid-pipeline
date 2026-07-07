with source as (
    select * from {{ source('raw', 'EIA_DEMAND_HOURLY_RAW') }}
)

select
    period_ts                          as demand_hour_ts,
    period_date                        as demand_date,
    respondent                         as balancing_authority,
    respondent_name                    as balancing_authority_name,
    try_cast(value as number(12,2))    as demand_mwh,
    _loaded_at
from source
where type = 'D'
