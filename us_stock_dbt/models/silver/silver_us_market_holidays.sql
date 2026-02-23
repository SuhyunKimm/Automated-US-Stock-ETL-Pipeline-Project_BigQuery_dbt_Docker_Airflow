-- us_market_holidays (silver layer)

{{ config (
	materialized='incremental',
	unique_key = 'date'
)}}

select
	cast(date as date) as date,
	status as status,
	datetime(concat(date, ' ', startTime)) as startTime,
	datetime(concat(date, ' ', endTime)) as endTime,
	description as description,
	ingestedAt as ingestedAt
from {{ source('bronze', 'bronze_us_market_holidays') }}
where date is not null

{% if is_incremental() %}
	and date > (select max(date) from {{this}})
{% endif %}