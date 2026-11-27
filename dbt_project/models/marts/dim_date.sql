-- One row per calendar day. Also the MetricFlow time spine (see _marts.yml).
-- Covers the WideWorldImporters data (Jan 2013 - Jun 2016) with room on both sides.
-- WWI's fiscal year starts on 1 November: Nov 2015 - Oct 2016 is fiscal year 2016.
with spine as (
    {{ dbt.date_spine(
        datepart="day",
        start_date="cast('2012-01-01' as date)",
        end_date="cast('2018-01-01' as date)"
    ) }}
),

days as (
    select cast(date_day as date) as date_day from spine
)

select
    date_day,
    cast(extract(year from date_day) as integer)             as calendar_year,
    cast(extract(quarter from date_day) as integer)          as calendar_quarter,
    cast(extract(month from date_day) as integer)            as calendar_month,
    cast(extract(day from date_day) as integer)              as day_of_month,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date)   as month_start_date,
    cast({{ dbt.date_trunc('quarter', 'date_day') }} as date) as quarter_start_date,
    cast(extract(year from date_day) as integer)
        + case when extract(month from date_day) >= 11 then 1 else 0 end as fiscal_year
from days
