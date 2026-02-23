-- scd snapshot for dim_ticker

{% snapshot tickers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='ticker',
        strategy ='check',
        check_cols=[
            'companyName',
            'country',
            'industry',
   	        'market',
            'currency'
        ]
    )
}}

select * from {{ ref('silver_tickers')}}

{% endsnapshot %}