{{ config(materialized='table') }}

WITH base AS (
  SELECT
    game_id,
    season,
    week,
    posteam,
    defteam,
    drive,
    play_id,
    play_type,
    down,
    ydstogo,
    fixed_drive_result,
    {{ yardline_100_num('yrdln','posteam') }} AS yardline_100_num
  FROM {{ ref('stg_pbp') }}
  WHERE posteam IS NOT NULL
),

pos AS (
  SELECT
    *,
    -- Potential FG distance approximation
    (yardline_100_num + 17) AS est_fg_distance,
    -- Zones
    CASE WHEN yardline_100_num <= 20 AND DOWN IN (1,2,3,4) THEN 1 ELSE 0 END AS in_redzone,
    CASE WHEN yardline_100_num BETWEEN 1 AND 44 AND DOWN IN (1,2,3,4) THEN 1 ELSE 0 END AS in_fg_range,
    CASE WHEN yardline_100_num BETWEEN 1 AND 44 AND down = 4 AND play_type IN ('run', 'pass') and ydstogo <=3 THEN 1 END AS fringe_go,
    CASE WHEN yardline_100_num BETWEEN 1 AND 44 AND down = 4 AND ydstogo <=3 THEN 1 END AS fringe_situations
  FROM base
)

SELECT *
FROM pos