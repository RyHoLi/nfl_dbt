-- models/staging/stg_draft_picks.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'draft_picks_data') }}
)

select
  cast(season as integer) as season,
  cast(round as integer) as round,
  cast(pick as integer) as pick,
  cast(team as varchar) as team,
  cast(gsis_id as varchar) as gsis_id,
  cast(pfr_player_id as varchar) as pfr_player_id,
  cast(cfb_player_id as varchar) as cfb_player_id,
  cast(pfr_player_name as varchar) as pfr_player_name,
  cast(hof as boolean) as hall_of_fame,
  cast(position as varchar) as position,
  cast(category as varchar) as category,
  cast(side as varchar) as side,
  cast(college as varchar) as college,
  cast(age as integer) as age,
  cast(until as integer) as until,
  cast(allpro as integer) as all_pro,
  cast(probowls as integer) as pro_bowls,
  cast(seasons_started as integer) as seasons_started,
  cast(w_av as integer) as weighted_av,
  cast(car_av as varchar) as career_av,
  cast(dr_av as integer) as draft_round_av,
  cast(games as integer) as games,
  cast(pass_completions as integer) as pass_completions,
  cast(pass_attempts as integer) as pass_attempts,
  cast(pass_yards as integer) as pass_yards,
  cast(pass_tds as integer) as pass_tds,
  cast(pass_ints as integer) as pass_ints,
  cast(rush_atts as integer) as rush_attempts,
  cast(rush_yards as integer) as rush_yards,
  cast(rush_tds as integer) as rush_tds,
  cast(receptions as integer) as receptions,
  cast(rec_yards as integer) as receiving_yards,
  cast(rec_tds as integer) as receiving_tds,
  cast(def_solo_tackles as varchar) as def_solo_tackles,
  cast(def_ints as varchar) as def_ints,
  cast(def_sacks as varchar) as def_sacks
from src

