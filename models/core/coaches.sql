
{{ config(materialized='table') }}

with coaches as (
    SELECT 
        home_coach as head_coach, 
        HOME_TEAM AS team_coach,
        season, 
        game_date,
        game_id,
        week
    FROM {{ref("stg_pbp")}}
    UNION
    SELECT 
        away_coach as head_coach, 
        AWAY_TEAM as team_coach,
        season, 
        game_date, 
        game_id, 
        week
    FROM {{ref("stg_pbp")}}
)

select * from coaches
