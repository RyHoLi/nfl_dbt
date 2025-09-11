-- models/staging/stg_officials.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'officials_data') }}
)

select
  cast(game_id as varchar) as game_id,
  cast(off_pos as varchar) as official_position,
  cast(official_id as varchar) as official_id,
  cast(name as varchar) as name,
  cast(season as integer) as season
from src

