-- models/staging/stg_draft_values.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'draft_values_data') }}
)

select
  cast(pick as integer) as pick,
  cast(stuart as float) as stuart_value,
  cast(johnson as integer) as johnson_value,
  cast(hill as float) as hill_value,
  cast(otc as integer) as otc_value,
  cast(pff as float) as pff_value
from src

