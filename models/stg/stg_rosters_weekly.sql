-- models/staging/stg_rosters_weekly.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'rosters_weekly_data') }}
)

select
  cast(season as integer) as season,
  cast(team as varchar) as team,
  cast(position as varchar) as position,
  cast(depth_chart_position as varchar) as depth_chart_position,
  cast(jersey_number as integer) as jersey_number,
  cast(status as varchar) as status,
  cast(player_name as varchar) as player_name,
  cast(first_name as varchar) as first_name,
  cast(last_name as varchar) as last_name,
  cast(birth_date as date) as birth_date,
  cast(height as integer) as height,
  cast(weight as integer) as weight,
  cast(college as varchar) as college,
  cast(player_id as varchar) as player_id,
  cast(espn_id as integer) as espn_id,
  cast(sportradar_id as varchar) as sportradar_id,
  cast(yahoo_id as integer) as yahoo_id,
  cast(rotowire_id as integer) as rotowire_id,
  cast(pff_id as integer) as pff_id,
  cast(pfr_id as varchar) as pfr_id,
  cast(fantasy_data_id as integer) as fantasy_data_id,
  cast(sleeper_id as integer) as sleeper_id,
  cast(years_exp as integer) as years_experience,
  cast(headshot_url as varchar) as headshot_url,
  cast(ngs_position as varchar) as ngs_position,
  cast(week as integer) as week,
  cast(game_type as varchar) as game_type,
  cast(status_description_abbr as varchar) as status_description_abbr,
  cast(football_name as varchar) as football_name,
  cast(esb_id as varchar) as esb_id,
  cast(gsis_it_id as integer) as gsis_id,
  cast(smart_id as varchar) as smart_id,
  cast(entry_year as integer) as entry_year,
  cast(rookie_year as integer) as rookie_year,
  cast(draft_club as varchar) as draft_club,
  cast(draft_number as varchar) as draft_number,
  cast(age as float) as age
from src
