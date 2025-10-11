{{ config(materialized='table') }}

with schedule_week as (
    SELECT game_id, week, season, home_team as posteam, spread_line * -1 as spread, 1 as home,
    total_line, 
        CASE WHEN temperature IS NULL THEN 70 ELSE temperature END AS temp, 
        CASE WHEN wind IS NULL THEN 0 ELSE wind END AS wind, 
        roof,
        CASE WHEN roof IN ('closed', 'dome') THEN 1 ELSE 0 END AS indoor_game,
        CASE WHEN roof IN ('open', 'outdoors') THEN 1 ELSE 0 END AS outdoor_game
    from {{ ref('schedule')}}
    where season = 2025
        and week   = 6
    UNION
    SELECT game_id, week, season, away_team as posteam, spread_line * 1 as spread, 0 as home,
    total_line, 
        CASE WHEN temperature IS NULL THEN 70 ELSE temperature END AS temp, 
        CASE WHEN wind IS NULL THEN 0 ELSE wind END AS wind, 
        roof,
        CASE WHEN roof IN ('closed', 'dome') THEN 1 ELSE 0 END AS indoor_game,
        CASE WHEN roof IN ('open', 'outdoors') THEN 1 ELSE 0 END AS outdoor_game
    from {{ ref('schedule')}}
    where season = 2025
        and week   = 6
),
predict_scoring_table as (
  select *
  from {{ ref('predict_kicker_scoring') }}
  where season = 2025
    and week   = 5     -- lagged stats up through week 4
)

select
  a.kicker_player_id,
  a.kicker_player_name,

  -- teams forced from the schedule to ensure correctness
  s.posteam,
  s.home as home_flag,
  a.season,
  6 as week,          -- prediction week
  s.spread,
  s.total_line,
  s.temp,
  s.wind,
  s.indoor_game,
  s.outdoor_game,
  a.fringe_go_pct_lag,
  a.cum_num_drives_lag,
  a.cum_fg_range_drives_lag,
  a.cum_redzone_drives_lag,
  a.cum_opponent_drives_allowed_lag,
  a.cum_fga_0_39_lag,
  a.cum_fga_40_49_lag,
  a.cum_fga_50_lag,
  a.cum_extra_point_attempts_lag,
  a.fg_range_drives_pct_lag,
  a.redzone_drives_pct_lag,
  a.fourth_down_attempt_in_fg_range_pct_lag,
  a.fourth_down_attempt_in_redzone_pct_lag,
  a.fg_range_stall_pct_lag,
  a.opponent_redzone_drives_allowed_pct_lag,
  a.opponent_fg_range_drives_allowed_pct_lag,
  a.opponent_fg_attempts_allowed_0_39_pct_lag,
  a.opponent_fg_attempts_allowed_40_49_pct_lag,
  a.opponent_fg_attempts_allowed_50_pct_lag,
  a.opponent_xp_attempts_allowed_pct_lag,
  a.extra_point_made_pct_lag,
  a.extra_point_made_career_pct_lag,
  a.fgm_0_39_career_pct_lag,
  a.fgm_40_49_career_pct_lag,
  a.fgm_50_career_pct_lag,
  a.cum_fantasy_pts_season_lag,
  a.actual_fantasy_pts::decimal(5,2) as actual_fantasy_pts
from predict_scoring_table a
join schedule_week s
  on a.season = s.season
 and a.posteam = s.posteam
