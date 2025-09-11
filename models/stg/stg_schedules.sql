-- models/staging/stg_schedules.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'schedules_data') }}
)

select
  cast(game_id as varchar) as game_id,
  cast(season as integer) as season,
  cast(game_type as varchar) as game_type,
  cast(week as integer) as week,
  cast(gameday as date) as gameday,
  cast(weekday as varchar) as weekday,
  cast(gametime as time) as gametime,
  cast(away_team as varchar) as away_team,
  cast(away_score as integer) as away_score,
  cast(home_team as varchar) as home_team,
  cast(home_score as integer) as home_score,
  cast(location as varchar) as location,
  cast(result as integer) as result,
  cast(total as integer) as total,
  cast(overtime as integer) as overtime,
  cast(old_game_id as integer) as old_game_id,
  cast(gsis as integer) as gsis,
  cast(nfl_detail_id as varchar) as nfl_detail_id,
  cast(pfr as varchar) as pfr,
  cast(pff as integer) as pff,
  cast(espn as integer) as espn,
  cast(ftn as varchar) as ftn,
  cast(away_rest as integer) as away_rest,
  cast(home_rest as integer) as home_rest,
  cast(away_moneyline as integer) as away_moneyline,
  cast(home_moneyline as integer) as home_moneyline,
  cast(spread_line as float) as spread_line,
  cast(away_spread_odds as integer) as away_spread_odds,
  cast(home_spread_odds as integer) as home_spread_odds,
  cast(total_line as float) as total_line,
  cast(under_odds as integer) as under_odds,
  cast(over_odds as integer) as over_odds,
  cast(div_game as boolean) as division_game,
  cast(roof as varchar) as roof,
  cast(surface as varchar) as surface,
  cast(temp as varchar) as temperature,
  cast(wind as varchar) as wind,
  cast(away_qb_id as varchar) as away_qb_id,
  cast(home_qb_id as varchar) as home_qb_id,
  cast(away_qb_name as varchar) as away_qb_name,
  cast(home_qb_name as varchar) as home_qb_name,
  cast(away_coach as varchar) as away_coach,
  cast(home_coach as varchar) as home_coach,
  cast(referee as varchar) as referee,
  cast(stadium_id as varchar) as stadium_id,
  cast(stadium as varchar) as stadium
from src

