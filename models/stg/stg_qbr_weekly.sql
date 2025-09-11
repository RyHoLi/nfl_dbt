-- models/staging/stg_qbr_weekly.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'qbr_weekly_data') }}
)

select
  cast(season as integer) as season,
  cast(season_type as varchar) as season_type,
  cast(game_id as integer) as game_id,
  cast(game_week as integer) as game_week,
  cast(week_text as varchar) as week_text,
  cast(team_abb as varchar) as team_abbr,
  cast(player_id as integer) as player_id,
  cast(name_short as varchar) as name_short,
  cast(rank as integer) as rank,
  cast(qbr_total as float) as qbr_total,
  cast(pts_added as float) as points_added,
  cast(qb_plays as integer) as qb_plays,
  cast(epa_total as float) as epa_total,
  cast(pass as float) as pass,
  cast(run as float) as run,
  cast(exp_sack as boolean) as expected_sack,
  cast(penalty as float) as penalty,
  cast(qbr_raw as float) as qbr_raw,
  cast(sack as float) as sack,
  cast(name_first as varchar) as name_first,
  cast(name_last as varchar) as name_last,
  cast(name_display as varchar) as name_display,
  cast(headshot_href as varchar) as headshot_url,
  cast(team as varchar) as team_name,
  cast(opp_id as integer) as opponent_id,
  cast(opp_abb as varchar) as opponent_abbr,
  cast(opp_team as varchar) as opponent_team,
  cast(opp_name as varchar) as opponent_name,
  cast(week_num as integer) as week_number,
  cast(qualified as boolean) as qualified
from src