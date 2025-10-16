{{ config(materialized='table') }}

WITH opp_ops AS (
  SELECT game_id, season, week, posteam, defteam, num_drives, redzone_drives, fg_range_drives, 
  fg_range_drive_pct, redzone_drive_pct, fourth_down_fg_range_pct, fourth_down_redzone_pct, redzone_td_pct, fg_range_stall_pct
  FROM {{ ref('core_fg_range_opportunities') }}
),

fg_xp AS (
  SELECT
    game_id, season, week, posteam, defteam,
    SUM(fga_0_39)    AS fga_0_39_allowed,
    SUM(fga_40_49)   AS fga_40_49_allowed,
    SUM(fga_50) AS fga_50_plus_allowed,
    SUM(extra_point_attempts)         AS xpa_allowed,
  FROM {{ ref('kicker_weekly') }}
  GROUP BY 1,2,3,4,5
),

-- Passing & rushing allowed (by defense) from PBP
pass_rush_allowed AS (
  SELECT
    game_id, season, week, defteam,
    -- passing
    SUM(CASE WHEN pass_attempt = 1 THEN 1 ELSE 0 END)              AS pass_att_allowed,
    SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END)             AS pass_cmp_allowed,
    SUM(CASE WHEN passing_yards IS NOT NULL THEN passing_yards ELSE 0 END) AS pass_yds_allowed,
    SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END)            AS pass_tds_allowed,
    -- rushing
    SUM(CASE WHEN rush_attempt = 1 THEN 1 ELSE 0 END)              AS rush_att_allowed,
    SUM(CASE WHEN rushing_yards IS NOT NULL THEN rushing_yards ELSE 0 END) AS rush_yds_allowed,
    SUM(CASE WHEN rush_touchdown = 1 THEN 1 ELSE 0 END)            AS rush_tds_allowed,
    -- total plays
    SUM(CASE WHEN qb_dropback = 1 OR rush_attempt = 1 THEN 1 ELSE 0 END) AS off_plays_allowed
  FROM {{ ref('stg_pbp') }}
  GROUP BY 1,2,3,4
),

opp_k_fpts AS (
  SELECT
    k.game_id, k.season, k.week, k.defteam, fantasy_pts AS opponent_kicker_fpts_allowed
  FROM {{ ref('kicker_weekly') }} k
),

assemble AS (
  SELECT
    o.game_id,
    o.season,
    o.week,
    o.defteam AS defteam,
    o.posteam AS posteam,
    COALESCE(o.num_drives,0)       AS opponent_drives_allowed,
    COALESCE(o.redzone_drives,0)   AS opponent_redzone_drives_allowed,
    COALESCE(o.fg_range_drives,0)  AS opponent_fg_range_drives_allowed,
    COALESCE(fx.fga_0_39_allowed,0)     AS opponent_fg_attempts_allowed_0_39,
    COALESCE(fx.fga_40_49_allowed,0)     AS opponent_fg_attempts_allowed_40_49,
    COALESCE(fx.fga_50_plus_allowed,0)     AS opponent_fg_attempts_allowed_50_plus,
    COALESCE(fx.xpa_allowed,0)     AS opponent_xp_attempts_allowed,
    COALESCE(ok.opponent_kicker_fpts_allowed,0) AS opponent_kicker_fpts_allowed,
    -- pass/rush allowed
    pr.pass_att_allowed,
    pr.pass_cmp_allowed,
    pr.pass_yds_allowed,
    pr.pass_tds_allowed,
    pr.rush_att_allowed,
    pr.rush_yds_allowed,
    pr.rush_tds_allowed,
    pr.off_plays_allowed
  FROM opp_ops o
  LEFT JOIN fg_xp fx
    ON o.game_id = fx.game_id AND o.season = fx.season AND o.week = fx.week AND o.posteam = fx.posteam
  LEFT JOIN opp_k_fpts ok
    ON o.game_id = ok.game_id AND o.season = ok.season AND o.week = ok.week AND o.defteam = ok.defteam
  LEFT JOIN pass_rush_allowed pr
    ON o.game_id = pr.game_id AND o.season = pr.season AND o.week = pr.week AND o.defteam = pr.defteam
)

SELECT * FROM assemble
