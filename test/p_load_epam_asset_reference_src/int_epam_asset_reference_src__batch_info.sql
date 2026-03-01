-- models/intermediate/int_epam_asset_reference_src__batch_info.sql
{{
    config(
        materialized='ephemeral'
    )
}}

with batch_date as (

    select * from {{ ref('batch_date') }}

)

select
    batch_label,
    batch_date as effective_date
from batch_date
where batch_label_type = '{{ var("p_batch_label_type", "EOD") }}'