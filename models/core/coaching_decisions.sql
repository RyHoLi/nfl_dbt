{{ config(materialized='table') }}

with plays as (
    select
        play_id,
        WEEK,
        season,
        game_id,
        home_coach,
        away_coach,
        posteam,
        defteam,
        quarter_seconds_remaining,
        half_seconds_remaining,
        game_seconds_remaining,
        qtr,
        down,
        ydstogo,
        yardline_100,
        score_differential,
        play_type,
        field_goal_attempt,
        field_goal_result,
        play_desc,
        home_score,
        away_score,
        vegas_total,
        spread_line,
        total_line,
        roof,
        surface,
        series_result,
        wp,
        def_wp,
        home_wp,
        away_wp,
        wpa,
        -- Determine which coach is responsible for the decision
        case
            when posteam = home_team then home_coach
            when posteam = away_team then away_coach
            else null
        end as decision_coach,
        -- Situation features
        case when down = 4 then 1 else 0 end as is_fourth_down,
        -- Go-for-it: 4th down, not a field goal, not a punt
        case
            when down = 4 and play_type in ('run', 'pass') then 1
            else 0
        end as went_for_it,
        -- Field goal attempt on 4th down
        case
            when down = 4 and play_type = 'field_goal' then 1
            else 0
        end as fg_attempted,
        case
            when yardline_100 <= 48 AND down = 4 and play_type in ('run', 'pass') THEN 1
            ELSE 0
        END AS went_for_it_in_fg_range,
        CASE
            WHEN yardline_100 <= 20 AND down = 4 AND play_type in ('run', 'pass') THEN 1
            ELSE 0
        END AS went_for_it_in_redzone,
        CASE
            WHEN yardline_100 <= 45 AND down = 4 AND play_type in ('run', 'pass') AND ydstogo <= 5 THEN 1
            ELSE 0
        END AS fringe_go,
        CASE
            WHEN yardline_100 <= 45 AND down = 4 AND ydstogo <= 5 THEN 1
            ELSE 0
        END AS fringe_situations
    from {{ ref('stg_pbp') }}
    where down = 4
      and posteam is not null
)

select
    decision_coach as coach,
    play_id,
    game_id,
    week,
    season,
    posteam,
    defteam,
    qtr,
    quarter_seconds_remaining,
    half_seconds_remaining,
    game_seconds_remaining,
    ydstogo,
    yardline_100,
    score_differential,
    home_score,
    away_score,
    vegas_total,
    spread_line,
    total_line,
    roof,
    surface,
    series_result,
    wp,
    def_wp,
    home_wp,
    away_wp,
    wpa,
    went_for_it,
    fg_attempted,
    went_for_it_in_fg_range,
    went_for_it_in_redzone,
    field_goal_result,
    play_desc,
    fringe_go,
    fringe_situations
from plays
where decision_coach is not null


