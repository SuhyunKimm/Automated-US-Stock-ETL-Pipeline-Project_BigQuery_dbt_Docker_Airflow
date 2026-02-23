-- dim_date (gold layer)

{{
    config(
        materialized = "table"
    )
}}

with date_cte as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2021-01-01' as date)",
        end_date="date_add(current_date(), interval 10 year)"
    ) }}
)
select
    cast(format_date('%Y%m%d', date_day) as int64) as dateKey,
    cast(date_day as date) as fullDate,
    extract(day from date_day) as dayOfMonth,
    format_date('%A', date_day) as dayName,
    extract(dayofweek from date_day) as dayOfWeek,
    extract(dayofyear from date_day) as dayOfYear,
    extract(week from date_day) as weekOfYear,
    format_date('%B', date_day) as monthName,
    extract(month from date_day)  as monthNum,
    extract(quarter from date_day) as quarter,
    extract(year from date_day) as year,
    case
        when extract(dayofweek from date_day) in (1,7) then true
        else false
    end as isWeekend,
    case
        when h.date is not null then true
        else false
    end as isHoliday,
    case
        when h.status is null then null
        when h.status = 'short day' then true
        else false
    end as isShortDay
from date_cte dc
left outer join {{ ref('silver_us_market_holidays') }} h
on dc.date_day = h.date