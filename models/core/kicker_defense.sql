{{ config(materialized='table') }}

with schedule AS (
    SELECT
        CASE WHEN k.posteam = s.home_team THEN s.away_team
             ELSE s.home_team
             END AS def_team,
        k.game_id,
        k.week,
        k.season,
        field_goal_attempt,
        field_goal_made,
        field_goal_distance,
        extra_point_attempt,
        extra_point_made,
        CASE WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 0 and 39 THEN 3
             WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance BETWEEN 40 and 49 THEN 4
             WHEN field_goal_attempt = 1 AND field_goal_made = 1 AND field_goal_distance >= 50 THEN 5
             WHEN extra_point_attempt = 1 AND extra_point_made = 1 THEN 1
             ELSE 0 END AS fantasy_pts_allowed
    FROM {{ ref('kickers_pbp') }} k
    LEFT JOIN {{ ref('schedule')}} s
        ON k.game_id = s.game_id 
    WHERE play_type IN ('field_goal', 'extra_point')
), aggregate_weekly_schedule AS (
    SELECT
        def_team,
        game_id,
        week,
        season,
        SUM(field_goal_attempt) AS field_goal_attempts,
        SUM(field_goal_made) AS field_goals_made,
        COALESCE(SUM(field_goal_made) / NULLIF(SUM(field_goal_attempt),0),0) AS FIELD_GOAL_MADE_PCT,
        SUM(extra_point_attempt) AS extra_point_attempts,
        SUM(extra_point_made) AS extra_point_made,
        COALESCE(SUM(extra_point_made) / NULLIF(SUM(extra_point_attempt),0),0) AS EXTRA_POINT_MADE_PCT,
        SUM(fantasy_pts_allowed) AS fantasy_pts_allowed,
        AVG(CASE WHEN field_goal_made = 1 THEN field_goal_distance END) AS avg_field_goal_dist_made,
        AVG(CASE WHEN field_goal_attempt = 1 THEN field_goal_distance END) AS avg_field_goal_dist_attempt
    FROM schedule
    GROUP BY 1,2,3,4
)
select * from aggregate_weekly_schedule