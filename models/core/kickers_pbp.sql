{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref("stg_pbp") }}
    where play_type in ('field_goal', 'extra_point') or kicker_player_id is not null
)

select
    cast(kicker_player_id as varchar) as kicker_player_id,
    cast(kicker_player_name as varchar) as kicker_player_name,
    cast(game_id as varchar) as game_id,
    cast(week as integer) as week,
    cast(season as integer) as season,
    cast(posteam as varchar) as posteam,
    cast(defteam as varchar) as defteam,
    home_team,
    away_team,
    cast(play_id as integer) as play_id,
    drive,
    drive_play_id_ended,
    cast(play_type as varchar) as play_type,
    cast(kick_distance as integer) as field_goal_distance,
    cast(field_goal_result as varchar) as field_goal_result,
    cast(extra_point_result as varchar) as extra_point_result,
    -- Attempt flags
    case when play_type = 'field_goal' then 1 else 0 end as field_goal_attempt,
    case when play_type = 'extra_point' then 1 else 0 end as extra_point_attempt,
    -- Made flags
    case when play_type = 'field_goal' and field_goal_result = 'made' then 1 else 0 end as field_goal_made,
    case when play_type = 'extra_point' and extra_point_result = 'good' then 1 else 0 end as extra_point_made,
    -- Missed flags
    case when play_type = 'field_goal' and field_goal_result in ('missed', 'blocked') then 1 else 0 end as field_goal_missed,
    case when play_type = 'extra_point' and extra_point_result in ('missed', 'blocked') then 1 else 0 end as extra_point_missed,
    -- Blocked flags
    case when play_type = 'field_goal' and field_goal_result = 'blocked' then 1 else 0 end as field_goal_blocked,
    case when play_type = 'extra_point' and extra_point_result = 'blocked' then 1 else 0 end as extra_point_blocked,
    -- Two point conversion attempt by kicker (rare, but possible)
    case when two_point_attempt = 1 and kicker_player_id is not null then 1 else 0 end as two_point_attempt_by_kicker,
    -- Kickoff stats (if available)
    cast(kickoff_attempt as integer) as kickoff_attempt,
    cast(touchback as integer) as touchback,
    cast(kickoff_out_of_bounds as integer) as kickoff_out_of_bounds,
    cast(kickoff_downed as integer) as kickoff_downed,
    cast(kickoff_fair_catch as integer) as kickoff_fair_catch,
    cast(kickoff_inside_twenty as integer) as kickoff_inside_twenty,
    cast(kickoff_in_endzone as integer) as kickoff_in_endzone
from src
where
    (play_type in ('field_goal', 'extra_point') or kicker_player_id is not null)