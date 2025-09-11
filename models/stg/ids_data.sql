-- models/staging/stg_ids.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'ids_data') }}
)

select
  cast(gsis_id as varchar) as gsis_id,
  cast(sleeper_id as varchar) as sleeper_id,
  cast(draft_round as varchar) as draft_round,
  cast(stats_id as varchar) as stats_id,
  cast(position as varchar) as position,
  cast(draft_year as integer) as draft_year,
  cast(age as integer) as age,
  cast(cbs_id as varchar) as cbs_id,
  cast(twitter_username as varchar) as twitter_username,
  cast(name as varchar) as name,
  cast(pfr_id as varchar) as pfr_id,
  cast(mfl_id as integer) as mfl_id,
  cast(rotowire_id as varchar) as rotowire_id,
  cast(ktc_id as varchar) as ktc_id,
  cast(stats_global_id as integer) as stats_global_id,
  cast(college as varchar) as college,
  cast(fantasy_data_id as varchar) as fantasy_data_id,
  cast(yahoo_id as varchar) as yahoo_id,
  cast(fantasypros_id as varchar) as fantasypros_id,
  cast(merge_name as varchar) as merge_name,
  cast(draft_ovr as varchar) as draft_overall,
  cast(draft_pick as varchar) as draft_pick,
  cast(pff_id as varchar) as pff_id,
  cast(team as varchar) as team,
  cast(weight as integer) as weight,
  cast(birthdate as date) as birthdate,
  cast(espn_id as varchar) as espn_id,
  cast(height as integer) as height,
  cast(cfbref_id as varchar) as cfbref_id,
  cast(rotoworld_id as varchar) as rotoworld_id,
  cast(swish_id as varchar) as swish_id,
  cast(nfl_id as varchar) as nfl_id,
  cast(fleaflicker_id as varchar) as fleaflicker_id,
  cast(db_season as integer) as db_season,
  cast(sportradar_id as varchar) as sportradar_id
from src

