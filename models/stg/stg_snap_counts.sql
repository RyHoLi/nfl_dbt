-- models/staging/stg_snap_counts_data.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'snap_counts_data') }}
)

select
  cast(game_id as varchar) as game_id,
  cast(pfr_game_id as varchar) as pfr_game_id,
  cast(season as integer) as season,
  cast(game_type as varchar) as game_type,
  cast(week as integer) as week,
  cast(player as varchar) as player,
  cast(pfr_player_id as varchar) as pfr_player_id,
  cast(position as varchar) as position,
  cast(team as varchar) as team,
  cast(opponent as varchar) as opponent,
  cast(offense_snaps as integer) as offense_snaps,
  cast(offense_pct as float) as offense_pct,
  cast(defense_snaps as integer) as defense_snaps,
  cast(defense_pct as float) as defense_pct,
  cast(st_snaps as integer) as st_snaps,
  cast(st_pct as float) as st_pct,
  cast(progress as boolean) as in_progress
from src


