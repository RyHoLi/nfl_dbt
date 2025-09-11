-- models/staging/stg_sc_lines.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'sc_lines_data') }}
)

select
  cast(season as integer) as season,
  cast(week as boolean) as week,
  cast(away_team as varchar) as away_team,
  cast(home_team as varchar) as home_team,
  cast(game_id as integer) as game_id,
  cast(side as varchar) as side,
  cast(line as number) as line
from src

