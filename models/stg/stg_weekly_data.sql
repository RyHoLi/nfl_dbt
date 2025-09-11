-- models/staging/stg_weekly_data.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'weekly_data') }}
)

select
  cast(player_id as varchar) as player_id,
  cast(player_name as varchar) as player_name,
  cast(player_display_name as varchar) as player_display_name,
  cast(position as varchar) as position,
  cast(position_group as varchar) as position_group,
  cast(headshot_url as varchar) as headshot_url,
  cast(recent_team as varchar) as recent_team,
  cast(season as integer) as season,
  cast(week as integer) as week,
  cast(season_type as varchar) as season_type,
  cast(opponent_team as varchar) as opponent_team,
  cast(completions as integer) as completions,
  cast(attempts as integer) as attempts,
  cast(passing_yards as integer) as passing_yards,
  cast(passing_tds as integer) as passing_tds,
  cast(interceptions as integer) as interceptions,
  cast(sacks as integer) as sacks,
  cast(sack_yards as integer) as sack_yards,
  cast(sack_fumbles as integer) as sack_fumbles,
  cast(sack_fumbles_lost as integer) as sack_fumbles_lost,
  cast(passing_air_yards as integer) as passing_air_yards,
  cast(passing_yards_after_catch as integer) as passing_yards_after_catch,
  cast(passing_first_downs as integer) as passing_first_downs,
  cast(passing_epa as float) as passing_epa,
  cast(passing_2pt_conversions as integer) as passing_2pt_conversions,
  cast(pacr as float) as pacr,
  cast(dakota as float) as dakota,
  cast(carries as integer) as carries,
  cast(rushing_yards as integer) as rushing_yards,
  cast(rushing_tds as integer) as rushing_tds,
  cast(rushing_fumbles as integer) as rushing_fumbles,
  cast(rushing_fumbles_lost as integer) as rushing_fumbles_lost,
  cast(rushing_first_downs as integer) as rushing_first_downs,
  cast(rushing_epa as float) as rushing_epa,
  cast(rushing_2pt_conversions as integer) as rushing_2pt_conversions,
  cast(receptions as integer) as receptions,
  cast(targets as integer) as targets,
  cast(receiving_yards as integer) as receiving_yards,
  cast(receiving_tds as integer) as receiving_tds,
  cast(receiving_fumbles as integer) as receiving_fumbles,
  cast(receiving_fumbles_lost as integer) as receiving_fumbles_lost,
  cast(receiving_air_yards as integer) as receiving_air_yards,
  cast(receiving_yards_after_catch as integer) as receiving_yards_after_catch,
  cast(receiving_first_downs as integer) as receiving_first_downs,
  cast(receiving_epa as float) as receiving_epa,
  cast(receiving_2pt_conversions as integer) as receiving_2pt_conversions,
  cast(racr as float) as racr,
  cast(target_share as float) as target_share,
  cast(air_yards_share as float) as air_yards_share,
  cast(wopr as float) as wopr,
  cast(special_teams_tds as integer) as special_teams_tds,
  cast(fantasy_points as float) as fantasy_points,
  cast(fantasy_points_ppr as float) as fantasy_points_ppr
from src


