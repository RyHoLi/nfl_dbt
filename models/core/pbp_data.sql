
{{ config(materialized='table') }}

with cte as (
    SELECT *
    FROM {{ref("stg_pbp")}}
)

select * from cte
