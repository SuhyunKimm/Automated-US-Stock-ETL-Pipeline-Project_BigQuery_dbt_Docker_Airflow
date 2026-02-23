-- dim_ticker (gold layer)

{{ config(
    materialized='table'
)}}

with current_tickers as (
    select 
    {{ dbt_utils.generate_surrogate_key(['ticker', 'dbt_valid_from']) }} as surrogateKey,
    *
    from {{ ref('tickers_snapshot') }}
    where dbt_valid_to is null
),
ticker_ids as (
    select
        row_number() over (order by ticker) as ticker_id,
        ticker
    from current_tickers
)
select
    ti.ticker_id as tickerId,
    ti.ticker,
    ct.surrogateKey,
    ct.companyName,
    ct.country,
    ct.industry,
   	ct.market,
    ct.currency
from current_tickers ct
inner join ticker_ids ti
on ct.ticker = ti.ticker