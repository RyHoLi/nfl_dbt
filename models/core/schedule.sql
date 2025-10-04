{{ config(materialized='table') }}

with schedule as (
    select * from {{ ref('stg_schedules') }}
)
select * from schedule