-- tickers (silver layer)

{{ config (
	materialized='table'
)}}

select
    upper(trim(ticker)) as ticker,
    companyName as companyName,
    country as country,
    industry as industry,
   	market as market,
    currency as currency,
    ingestedAt as ingestedAt
from {{ source('bronze', 'bronze_tickers') }}
where ticker is not null and trim(ticker) != ''