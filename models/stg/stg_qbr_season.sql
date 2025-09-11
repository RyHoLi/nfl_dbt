{{ config(materialized='view') }}

with qbr_season_source as (
  select *
  from {{ source('RAW', 'qbr_season_data') }}
)

select
  cast(season as integer) as season,
  cast(season_type as varchar) as season_type,
  cast(game_week as varchar) as game_week,
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
  cast(qualified as boolean) as qualified
from qbr_season_source