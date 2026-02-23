-- fact us stock table (gold layer)

{{ config(
    materialized='incremental',
    unique_key = ['ticker', 'date']
)}}

with base as (
    select
        d.dateKey as dateKey,
        t.tickerId as tickerId,
        s.open as openPrice,
        s.high as highPrice,
        s.low as lowPrice,
        s.close as closePrice,
        s.volume as volume
    from 
        {{ ref('silver_us_stocks') }} s
    inner join 
        {{ ref('dim_date') }} d
    on s.date = d.fullDate
    inner join 
        {{ ref('dim_tickers') }} t
    on s.ticker = t.ticker
), rtn as (
    select *,
        case 
            when lag(closePrice) over (partition by tickerId order by dateKey) is null
            then null
        else
        (closePrice - lag(closePrice) over (partition by tickerId order by dateKey))
        / lag(closePrice) over (partition by tickerId order by dateKey)
        end as dailyReturn,
        (closePrice / first_value(closePrice) over (partition by tickerId order by dateKey)) - 1 as cumulativeReturn
    from base
), vol as (
    select *,
        stddev(dailyReturn) over (partition by tickerId order by dateKey rows between 19 preceding and current row) as volatility20d
    from rtn
)
select * from vol
{% if is_incremental() %}
  where dateKey > (select max(dateKey) from {{ this }})
{% endif %}