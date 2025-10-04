{{ config(materialized='table') }}

with schedule AS (
    SELECT
        kicker_player_id,
        kicker_player_name,
        game_id,
        week,
        season,
        posteam,
        defteam,
        home_team,
        away_team,
        field_goal_attempt,
        field_goal_made,
        field_goal_distance,
        extra_point_attempt,
        extra_point_made,
        CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 0 and 39 THEN 3
             WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 40 and 49 THEN 4
             WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance >= 50 THEN 5
             WHEN extra_point_attempt = 1 AND extra_point_made = 1 THEN 1
             ELSE 0 END AS fantasy_pts
    FROM {{ ref('kickers_pbp') }}
    WHERE play_type IN ('field_goal', 'extra_point')
), aggregate_weekly_schedule AS (
    SELECT
        kicker_player_id,
        kicker_player_name,
        game_id,
        week,
        season,
        posteam,
        defteam,
        home_team,
        away_team,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_distance BETWEEN 0 and 39 THEN 1 ELSE 0 END) AS fga_0_39,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_distance BETWEEN 40 and 49 THEN 1 ELSE 0 END) AS fga_40_49,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_distance >= 50 THEN 1 ELSE 0 END) AS fga_50,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 0 and 39 THEN 1 ELSE 0 END) AS fgm_0_39,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 40 and 49 THEN 1 ELSE 0 END) AS fgm_40_49,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance >=50 THEN 1 ELSE 0 END) AS fgm_50,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 0 and 39 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_distance BETWEEN 0 and 39 THEN 1 ELSE 0 END), 0) AS fgm_pct_0_39,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 40 and 49 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_distance BETWEEN 40 and 49 THEN 1 ELSE 0 END), 0) AS fgm_pct_40_49,
        SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance >=50 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN field_goal_attempt = 1 AND field_goal_distance >= 50 THEN 1 ELSE 0 END), 0) AS fgm_pct_50,
        SUM(extra_point_attempt) AS extra_point_attempts,
        SUM(extra_point_made) AS extra_point_made,
        COALESCE(SUM(extra_point_made) / NULLIF(SUM(extra_point_attempt),0),0) AS EXTRA_POINT_MADE_PCT,
        SUM(fantasy_pts) AS fantasy_pts,
    FROM schedule
    GROUP BY 1,2,3,4,5,6,7,8,9
)
select * from aggregate_weekly_schedule