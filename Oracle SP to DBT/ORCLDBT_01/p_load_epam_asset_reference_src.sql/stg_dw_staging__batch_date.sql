-- models/staging/stg_dw_staging__batch_date.sql
{{ config(
    materialized='view',
    tags=['staging', 'batch_date']
) }}

with source_data as (
    select * from {{ source('dw_staging', 'BATCH_DATE') }}
)

, renamed_casted as (
    select
        batch_label,
        batch_date,
        batch_label_type
    from source_data
)

select * from renamed_casted;