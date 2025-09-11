-- models/staging/stg_depth_charts.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'depth_charts_data') }}
)

select
  cast(season as integer) as season,
  cast(club_code as varchar) as club_code,
  cast(week as integer) as week,
  cast(game_type as varchar) as game_type,
  cast(depth_team as integer) as depth_team,
  cast(last_name as varchar) as last_name,
  cast(first_name as varchar) as first_name,
  cast(football_name as varchar) as football_name,
  cast(formation as varchar) as formation,
  cast(gsis_id as varchar) as gsis_id,
  cast(jersey_number as integer) as jersey_number,
  cast(position as varchar) as position,
  cast(elias_id as varchar) as elias_id,
  cast(depth_position as varchar) as depth_position,
  cast(full_name as varchar) as full_name
from src

