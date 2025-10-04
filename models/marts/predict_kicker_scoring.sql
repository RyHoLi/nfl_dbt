{{ config(materialized='table') }}

with kickers as (
    SELECT * FROM {{ ref('kicker_weekly')}}
),
fg_opportunities as (
    SELECT * FROM {{ ref('core_fg_range_opportunities')}}
),
team_defense as (
    SELECT * FROM {{ ref('core_team_defense_allowed')}}
),
coach_decisions as (
    SELECT * FROM {{ ref('coaching_decisions')}}
),
pregame as (
    SELECT game_id, week, season, home_team as posteam, spread_line * -1 as spread, (total_line/2) + (spread_line) / 2 as team_total, total_line, 
    CASE WHEN temp IS NULL THEN 70 ELSE temp END AS temp, 
    CASE WHEN wind IS NULL THEN 0 ELSE wind END AS wind, 
    roof,
    CASE WHEN roof IN ('closed', 'dome') THEN 1 ELSE 0 END AS indoor_game,
    CASE WHEN roof IN ('open', 'outdoors') THEN 1 ELSE 0 END AS outdoor_game
    FROM dev_stg.stg_pbp
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    UNION 
    SELECT game_id, week, season, away_team as posteam, spread_line as spread, (total_line/2) - (spread_line) / 2 as team_total, total_line, 
    CASE WHEN temp IS NULL THEN 70 ELSE temp END AS temp,
    CASE WHEN wind IS NULL THEN 0 ELSE wind END AS wind,
    roof,
    CASE WHEN roof IN ('closed', 'dome') THEN 1 ELSE 0 END AS indoor_game,
    CASE WHEN roof IN ('open', 'outdoors') THEN 1 ELSE 0 END AS outdoor_game
    FROM dev_stg.stg_pbp
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
defense_week as (
  select
    defteam,
    season,
    week,
    game_id,
    sum(coalesce(opponent_drives_allowed,0)) as drives_allowed,
    sum(coalesce(opponent_redzone_drives_allowed,0)) as redzone_drives_allowed,
    sum(COALESCE(opponent_fg_range_drives_allowed,0))  AS opponent_fg_range_drives_allowed,
    sum(COALESCE(opponent_fg_attempts_allowed_0_39,0))     AS opponent_fg_attempts_allowed_0_39,
    sum(COALESCE(opponent_fg_attempts_allowed_40_49,0))     AS opponent_fg_attempts_allowed_40_49,
    sum(COALESCE(opponent_fg_attempts_allowed_50_plus,0))     AS opponent_fg_attempts_allowed_50_plus,
    sum(COALESCE(opponent_xp_attempts_allowed,0))     AS opponent_xp_attempts_allowed
  from team_defense
  group by 1,2,3,4
),
defense_week_cum as (
  select
    defteam,
    season,
    week,
    game_id,
    sum(drives_allowed) over (partition by defteam, season order by week rows unbounded preceding ) as cum_opponent_drives_allowed,
    sum(redzone_drives_allowed) over (partition by defteam, season order by week rows unbounded preceding ) as cum_opponent_redzone_drives_allowed,
    sum(opponent_fg_range_drives_allowed) over (partition by defteam, season order by week rows unbounded preceding) as cum_opponent_fg_range_drives_allowed,
    sum(opponent_fg_attempts_allowed_0_39) over (partition by defteam, season order by week rows unbounded preceding) as cum_opponent_fg_attempts_allowed_0_39,
    sum(opponent_fg_attempts_allowed_40_49) over (partition by defteam, season order by week rows unbounded preceding) as cum_opponent_fg_attempts_allowed_40_49,
    sum(opponent_fg_attempts_allowed_50_plus) over (partition by defteam, season order by week rows unbounded preceding) as cum_opponent_fg_attempts_allowed_50_plus,
    sum(opponent_xp_attempts_allowed) over (partition by defteam, season order by week rows unbounded preceding) as cum_opponent_xp_attempts_allowed
  from defense_week
),
coaching_decisions_agg as (
    SELECT
        coach,
        game_id,
        posteam,
        season,
        week,
        SUM(fringe_go) as fringe_go,
        SUM(fringe_situations) as fringe_situations
    FROM coach_decisions
    GROUP BY 1,2,3,4,5
),
weekly_kicker_stats as (
    SELECT
        k.kicker_player_id,
        k.kicker_player_name,
        k.game_id,
        k.week,
        k.season,
        cd.coach,
        cd.fringe_go,
        cd.fringe_situations,
        k.posteam,
        CASE WHEN k.posteam = k.home_team THEN 1 ELSE 0 END AS home_flag,
        td.defteam,
        p.spread,
        p.total_line,
        p.temp,
        p.wind,
        p.indoor_game,
        p.outdoor_game,
        fo.num_drives,
        fo.redzone_drives,
        fo.fg_range_drives,
        fo.fourth_down_attempt_in_fg_range,
        fo.fourth_down_attempt_in_redzone,
        fo.fg_range_stall,
        fo.fg_range_drive_pct,
        fo.redzone_drive_pct,
        fo.fourth_down_fg_range_pct,
        fo.fourth_down_redzone_pct,
        fo.redzone_td_pct,
        fo.fg_range_stall_pct,
        td.cum_opponent_drives_allowed,
        td.cum_opponent_redzone_drives_allowed,
        td.cum_opponent_fg_range_drives_allowed,
        td.cum_opponent_fg_attempts_allowed_0_39,
        td.cum_opponent_fg_attempts_allowed_40_49,
        td.cum_opponent_fg_attempts_allowed_50_plus,
        td.cum_opponent_xp_attempts_allowed,
        k.fga_0_39,
        k.fga_40_49,
        k.fga_50,
        k.fgm_0_39,
        k.fgm_40_49,
        k.fgm_50,
        k.fgm_pct_0_39,
        k.fgm_pct_40_49,
        k.fgm_pct_50,
        k.extra_point_attempts,
        k.extra_point_made,
        k.extra_point_made_pct,
        k.fantasy_pts
    FROM kickers k
    LEFT JOIN fg_opportunities fo
        ON k.game_id = fo.game_id AND k.season = fo.season AND k.week = fo.week AND k.posteam = fo.posteam
    LEFT JOIN defense_week_cum td
        ON k.game_id = td.game_id AND k.season = td.season AND k.week = td.week AND k.defteam = td.defteam
    LEFT JOIN coaching_decisions_agg cd 
        ON cd.game_id = k.game_id AND cd.season = k.season AND cd.week = k.week AND cd.posteam = k.posteam
    LEFT JOIN pregame p
        ON k.game_id = p.game_id AND k.season = p.season AND k.week AND p.week AND k.posteam = p.posteam
),
kicker_model_season as (
    select
        kicker_player_id,
        kicker_player_name,
        game_id,
        week,
        coach,
        season,
        posteam,
        home_flag,
        defteam,
        spread,
        total_line,
        temp,
        wind,
        indoor_game,
        outdoor_game,
        fantasy_pts,
        SUM(fringe_go) OVER (PARTITION BY coach ORDER BY coach, season, week ROWS UNBOUNDED PRECEDING) as cum_fringe_go,
        SUM(fringe_situations) OVER (PARTITION BY coach ORDER BY coach, season, week ROWS UNBOUNDED PRECEDING) as cum_fringe_situations,
        SUM(num_drives) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week ROWS UNBOUNDED PRECEDING) as cum_num_drives,
        SUM(fg_range_drives) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week ROWS UNBOUNDED PRECEDING) as cum_fg_range_drives,
        SUM(redzone_drives) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week ROWS UNBOUNDED PRECEDING) as cum_redzone_drives,
        SUM(fourth_down_attempt_in_fg_range) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week ROWS UNBOUNDED PRECEDING) as cum_fourth_down_attempt_in_fg_range,
        SUM(fourth_down_attempt_in_redzone) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week ROWS UNBOUNDED PRECEDING) as cum_fourth_down_attempt_in_redzone,
        SUM(fg_range_stall) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week ROWS UNBOUNDED PRECEDING) as cum_fg_range_stall,
        cum_opponent_drives_allowed,
        cum_opponent_redzone_drives_allowed,
        cum_opponent_fg_range_drives_allowed,
        cum_opponent_fg_attempts_allowed_0_39,
        cum_opponent_fg_attempts_allowed_40_49,
        cum_opponent_fg_attempts_allowed_50_plus,
        cum_opponent_xp_attempts_allowed,
        SUM(fga_0_39) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fga_0_39,
        SUM(fga_40_49) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fga_40_49,
        SUM(fga_50) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fga_50,
        SUM(fgm_0_39) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_0_39,
        SUM(fgm_40_49) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_40_49,
        SUM(fgm_50) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_50,
        SUM(extra_point_attempts) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_extra_point_attempts,
        SUM(extra_point_made) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_extra_point_made,
        SUM(fantasy_pts) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fantasy_pts_season,
        SUM(extra_point_attempts) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_extra_point_attempts_career,
        SUM(extra_point_made) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_extra_point_made_career,
        SUM(fga_0_39) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fga_0_39_career,
        SUM(fga_40_49) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fga_40_49_career,
        SUM(fga_50) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fga_50_career,
        SUM(fgm_0_39) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_0_39_career,
        SUM(fgm_40_49) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_40_49_career,
        SUM(fgm_50) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_50_career,
        SUM(fgm_pct_0_39) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_pct_0_39_career,
        SUM(fgm_pct_40_49) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_pct_40_49_career,
        SUM(fgm_pct_50) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week ROWS UNBOUNDED PRECEDING) as cum_fgm_pct_50_career
    from weekly_kicker_stats
),
kicker_model_season_calc as (
    SELECT
        kicker_player_id,
        kicker_player_name,
        game_id,
        week,
        coach,
        season,
        posteam,
        home_flag,
        defteam,
        spread,
        total_line,
        temp,
        wind,
        indoor_game,
        outdoor_game,
        fantasy_pts,
        cum_fringe_situations,
        cum_num_drives,
        cum_fg_range_drives,
        cum_redzone_drives,
        cum_opponent_drives_allowed,
        cum_fga_0_39,
        cum_fga_40_49,
        cum_fga_50,
        cum_extra_point_attempts,
        cum_fantasy_pts_season,
        cum_extra_point_attempts_career,
        cum_fga_0_39_career,
        cum_fga_40_49_career,
        cum_fga_50_career,
        cum_fringe_go / NULLIF(cum_fringe_situations, 0) as fringe_go_pct ,
        cum_fg_range_drives / NULLIF(cum_num_drives, 0) as fg_range_drives_pct,
        cum_redzone_drives / NULLIF(cum_num_drives, 0) as redzone_drives_pct,
        cum_fourth_down_attempt_in_fg_range / NULLIF(cum_fg_range_drives, 0) as fourth_down_attempt_in_fg_range_pct,
        cum_fourth_down_attempt_in_redzone / NULLIF(cum_redzone_drives, 0) as fourth_down_attempt_in_redzone_pct,
        cum_fg_range_stall / NULLIF(cum_fg_range_drives, 0) as fg_range_stall_pct,
        cum_opponent_redzone_drives_allowed / NULLIF(cum_opponent_drives_allowed, 0) as opponent_redzone_drives_allowed_pct,
        cum_opponent_fg_range_drives_allowed / NULLIF(cum_opponent_drives_allowed, 0) as opponent_fg_range_drives_allowed_pct,
        cum_opponent_fg_attempts_allowed_0_39 / NULLIF(cum_opponent_drives_allowed, 0) as opponent_fg_attempts_allowed_0_39_pct,
        cum_opponent_fg_attempts_allowed_40_49 / NULLIF(cum_opponent_drives_allowed, 0) as opponent_fg_attempts_allowed_40_49_pct,
        cum_opponent_fg_attempts_allowed_50_plus / NULLIF(cum_opponent_drives_allowed, 0) as opponent_fg_attempts_allowed_50_pct,
        cum_opponent_xp_attempts_allowed / NULLIF(cum_opponent_drives_allowed, 0) as opponent_xp_attempts_allowed_pct,
        cum_fgm_0_39 / NULLIF(cum_fga_0_39, 0) as fgm_0_39_pct,
        cum_fgm_40_49 / NULLIF(cum_fga_40_49, 0) as fgm_40_49_pct,
        cum_fgm_50 / NULLIF(cum_fga_50, 0) as fgm_50_pct,
        cum_extra_point_made / NULLIF(cum_extra_point_attempts, 0) as extra_point_made_pct,
        cum_extra_point_made_career / NULLIF(cum_extra_point_attempts_career, 0) as extra_point_made_career_pct,
        cum_fgm_0_39_career / NULLIF(cum_fga_0_39_career, 0) as fgm_0_39_career_pct,
        cum_fgm_40_49_career / NULLIF(cum_fga_40_49_career, 0) as fgm_40_49_career_pct,
        cum_fgm_50_career / NULLIF(cum_fga_50_career, 0) as fgm_50_career_pct
    FROM kicker_model_season
),
kicker_model_lag as (
    SELECT
        kicker_player_id,
        kicker_player_name,
        posteam,
        home_flag,
        defteam,
        season,
        week,
        spread,
        total_line,
        temp,
        wind,
        indoor_game,
        outdoor_game,
        LAG(fringe_go_pct,1) OVER (PARTITION BY coach ORDER BY coach, season, week) as fringe_go_pct_lag,
        LAG(cum_num_drives,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as cum_num_drives_lag,
        LAG(cum_fg_range_drives,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as cum_fg_range_drives_lag,
        LAG(cum_redzone_drives,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as cum_redzone_drives_lag,
        LAG(cum_opponent_drives_allowed,1) OVER (PARTITION BY defteam, season ORDER BY defteam, season, week) as cum_opponent_drives_allowed_lag,
        LAG(cum_fga_0_39,1) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week) as cum_fga_0_39_lag,
        LAG(cum_fga_40_49,1) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week) as cum_fga_40_49_lag,
        LAG(cum_fga_50,1) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week) as cum_fga_50_lag,
        LAG(cum_extra_point_attempts,1) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week) as cum_extra_point_attempts_lag,
        LAG(fg_range_drives_pct,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as fg_range_drives_pct_lag,
        LAG(redzone_drives_pct,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as redzone_drives_pct_lag,
        LAG(fourth_down_attempt_in_fg_range_pct,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as fourth_down_attempt_in_fg_range_pct_lag,
        LAG(fourth_down_attempt_in_redzone_pct,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as fourth_down_attempt_in_redzone_pct_lag,
        LAG(fg_range_stall_pct,1) OVER (PARTITION BY posteam, season ORDER BY posteam, season, week) as fg_range_stall_pct_lag,
        LAG(opponent_redzone_drives_allowed_pct,1) OVER (PARTITION BY defteam, season ORDER BY defteam, season, week) as opponent_redzone_drives_allowed_pct_lag,
        LAG(opponent_fg_range_drives_allowed_pct,1) OVER (PARTITION BY defteam, season ORDER BY defteam, season, week) as opponent_fg_range_drives_allowed_pct_lag,
        LAG(opponent_fg_attempts_allowed_0_39_pct,1) OVER (PARTITION BY defteam, season ORDER BY defteam, season, week) as opponent_fg_attempts_allowed_0_39_pct_lag,
        LAG(opponent_fg_attempts_allowed_40_49_pct,1) OVER (PARTITION BY defteam, season ORDER BY defteam, season, week) as opponent_fg_attempts_allowed_40_49_pct_lag,
        LAG(opponent_fg_attempts_allowed_50_pct,1) OVER (PARTITION BY defteam, season ORDER BY defteam, season, week) as opponent_fg_attempts_allowed_50_pct_lag,
        LAG(opponent_xp_attempts_allowed_pct,1) OVER (PARTITION BY defteam, season ORDER BY defteam, season, week) as opponent_xp_attempts_allowed_pct_lag,
        LAG(extra_point_made_pct,1) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week) as extra_point_made_pct_lag,
        LAG(extra_point_made_career_pct,1) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week) as extra_point_made_career_pct_lag,
        LAG(fgm_0_39_career_pct,1) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week) as fgm_0_39_career_pct_lag,
        LAG(fgm_40_49_career_pct,1) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week) as fgm_40_49_career_pct_lag,
        LAG(fgm_50_career_pct,1) OVER (PARTITION BY kicker_player_id ORDER BY kicker_player_id, season, week) as fgm_50_career_pct_lag,
        LAG(cum_fantasy_pts_season,1) OVER (PARTITION BY kicker_player_id, season ORDER BY kicker_player_id, season, week) as cum_fantasy_pts_season_lag,
        fantasy_pts::DECIMAL(5,2) as actual_fantasy_pts
    FROM kicker_model_season_calc
)
select * from kicker_model_lag