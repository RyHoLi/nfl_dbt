{{ config(materialized='table') }}

WITH plays AS (
  SELECT
    game_id,
    season,
    week,
    posteam,
    defteam,
    kicker_player_id   AS kicker_id,
    kicker_player_name AS kicker_name,
    field_goal_attempt,
    extra_point_attempt,
    field_goal_result,
    extra_point_result,
    TRY_CAST(kick_distance as integer) AS kick_distance,
    CASE WHEN extra_point_attempt = 1 THEN 1 ELSE 0 END AS xpa,
    CASE WHEN extra_point_attempt = 1 AND extra_point_result = 'good' THEN 1 ELSE 0 END AS xpm,
    CASE WHEN field_goal_attempt = 1 AND field_goal_result = 'made' AND TRY_CAST(kick_distance as integer) < 40 THEN 1 ELSE 0 END AS fgm_0_39,
    CASE WHEN field_goal_attempt = 1 AND field_goal_result = 'made' AND TRY_CAST(kick_distance as integer) BETWEEN 40 AND 49 THEN 1 ELSE 0 END AS fgm_40_49,
    CASE WHEN field_goal_attempt = 1 AND field_goal_result = 'made' AND TRY_CAST(kick_distance as integer) >= 50 THEN 1 ELSE 0 END AS fgm_50_plus,
    CASE WHEN field_goal_attempt = 1 AND TRY_CAST(kick_distance as integer) < 40 THEN 1 ELSE 0 END AS fga_0_39,
    CASE WHEN field_goal_attempt = 1 AND TRY_CAST(kick_distance as integer) BETWEEN 40 AND 49 THEN 1 ELSE 0 END AS fga_40_49,
    CASE WHEN field_goal_attempt = 1 AND TRY_CAST(kick_distance as integer) >= 50 THEN 1 ELSE 0 END AS fga_50_plus
  FROM {{ ref('stg_pbp') }}
  WHERE (field_goal_attempt = 1 OR extra_point_attempt = 1)
)

SELECT * FROM plays
