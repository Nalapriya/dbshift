-- models/staging/stg_alcobev__primary_sales_actuals.sql
{{ config(
    materialized='view',
    tags=['staging', 'primary_sales_actuals']
) }}

with source_data as (
    select * from {{ source('alcobev', 'PRIMARY_SALES_ACTUALS') }}
)

, renamed_casted as (
    select
        "COMPANYCODE" as company_code,
        "STATECODE" as state_code,
        "SKUCODE" as sku_code,
        "VOLUMEACTUALCASE" as volume_actual_case,
        "CUSTOMERCODE" as customer_code
    from source_data
)

select * from renamed_casted