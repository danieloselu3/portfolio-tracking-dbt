{{
    config(
        materialized='ephemeral'
    )
}}

with src_data as (
    SELECT
    COUNTRY as COUNTRY_NAME -- TEXT
    , TRADINGHOURS as TRADING_HOURS -- TEXT
    , TIMEZONE as TIMEZONE -- TEXT
    , CITY as CITY_NAME -- TEXT
    , LOAD_TS as LOAD_TS -- TIMESTAMP_NTZ
    , EXCHANGECODE as EXCHANGE_CODE -- TEXT
    , NAME as EXCHANGE_NAME -- TEXT

    , 'SEED.ABC_Bank_EXCHANGE_INFO' as RECORD_SOURCE

    FROM {{ source('seeds', 'ABC_Bank_EXCHANGE_INFO') }}
),
default_data as (
    SELECT
    'Missing' as COUNTRY_NAME
    , 'Missing' as TRADING_HOURS
    , 'Missing' as TIMEZONE
    , 'Missing' as CITY_NAME
    , '2000-01-01' as LOAD_TS
    , 'Missing' as EXCHANGE_CODE
    , 'Missing' as EXCHANGE_NAME
    , 'System.DefaultKey' as RECORD_SOURCE
),
with_default_data as (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_data
),
hashed as (
    SELECT
    concat_ws('|', COUNTRY_NAME, EXCHANGE_CODE) as EXCHANGE_HKEY
    , concat_ws('|', COUNTRY_NAME,TRADING_HOURS,TIMEZONE,CITY_NAME,LOAD_TS,EXCHANGE_CODE,EXCHANGE_NAME,RECORD_SOURCE) as EXCHANGE_HDIFF
    , *
    FROM with_default_data
)
select * from hashed