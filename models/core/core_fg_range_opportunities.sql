{{ config(materialized='table') }}

WITH pos AS (
  SELECT
    game_id, season, week, posteam, defteam, drive, down,
    in_fg_range, in_redzone, play_type, fixed_drive_result
  FROM {{ ref('core_field_position') }}
),

drive_flags AS (
  SELECT
    game_id, season, week, posteam, defteam, drive,
    MAX(in_fg_range)  AS reached_fg_range,
    MAX(in_redzone)   AS reached_redzone,
    SUM(CASE WHEN down = 4 AND in_fg_range = 1 AND play_type != 'field_goal' THEN 1 ELSE 0 END) AS fourth_down_attempt_in_fg_range,
    SUM(CASE WHEN down = 4 AND in_redzone  = 1 AND play_type != 'field_goal' THEN 1 ELSE 0 END) AS fourth_down_attempt_in_redzone,
    SUM(CASE WHEN down = 4 AND in_fg_range  = 1 THEN 1 ELSE 0 END) AS fourth_down_fg_range_opp,
    SUM(CASE WHEN down = 4 AND in_redzone  = 1 THEN 1 ELSE 0 END) AS fourth_down_redzone_opp,
    -- Red zone TD rate: 1 if drive reached redzone and had a touchdown, else 0
    MAX(CASE WHEN in_redzone = 1 AND fixed_drive_result = 'Touchdown' THEN 1 ELSE 0 END) AS redzone_td,
    -- Stall rate in FG range: 1 if drive reached FG range and ended with turnover on downs, else 0
    MAX(CASE WHEN in_fg_range = 1 AND fixed_drive_result in ('Turnover on downs', 'Turnover', 'Opp touchdown')  THEN 1 ELSE 0 END) AS fg_range_stall
  FROM pos
  GROUP BY 1,2,3,4,5,6
),

agg AS (
  SELECT
    game_id, season, week, posteam, defteam, 
    COUNT(DISTINCT drive)                                 AS num_drives,
    SUM(reached_redzone)                                    AS redzone_drives,
    SUM(reached_fg_range)                                   AS fg_range_drives,
    SUM(fg_range_stall)                                     AS fg_range_stall,
    SUM(fourth_down_attempt_in_fg_range)                                       AS fourth_down_attempt_in_fg_range,
    SUM(fourth_down_attempt_in_redzone)                                       AS fourth_down_attempt_in_redzone,
    SUM(reached_fg_range) / NULLIF(COUNT(DISTINCT drive), 0)                        AS fg_range_drive_pct,
    SUM(reached_redzone) / NULLIF(COUNT(DISTINCT drive), 0)                       AS redzone_drive_pct,
    SUM(fourth_down_attempt_in_fg_range) / NULLIF(SUM(fourth_down_fg_range_opp),0)         AS fourth_down_fg_range_pct,
    SUM(fourth_down_attempt_in_redzone) / NULLIF(SUM(fourth_down_redzone_opp),0)         AS fourth_down_redzone_pct,
    -- Calculate redzone touchdown percentage
    CASE 
      WHEN COUNT_IF(reached_redzone = 1) = 0 THEN NULL
      ELSE SUM(redzone_td) / COUNT_IF(reached_redzone = 1)
    END AS redzone_td_pct,
    -- Calculate FG range stall percentage
    CASE 
      WHEN COUNT_IF(reached_fg_range = 1) = 0 THEN NULL
      ELSE SUM(fg_range_stall) / COUNT_IF(reached_fg_range = 1)
    END AS fg_range_stall_pct,
  FROM drive_flags,
  GROUP BY 1,2,3,4,5
)

SELECT * FROM agg
