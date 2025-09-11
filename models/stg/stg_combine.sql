-- models/staging/stg_combine.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'combine_data') }}
)

select
  cast(season as integer) as season,
  cast(draft_year as integer) as draft_year,
  cast(draft_team as varchar) as draft_team,
  cast(draft_round as integer) as draft_round,
  cast(draft_ovr as integer) as draft_overall,
  cast(pfr_id as varchar) as pfr_id,
  cast(cfb_id as varchar) as cfb_id,
  cast(player_name as varchar) as player_name,
  cast(pos as varchar) as position,
  cast(school as varchar) as school,
  cast(ht as varchar) as height,
  cast(wt as integer) as weight,
  cast(forty as float) as forty_yard_dash,
  cast(bench as integer) as bench_press,
  cast(vertical as varchar) as vertical_jump,
  cast(broad_jump as varchar) as broad_jump,
  cast(cone as varchar) as cone_drill,
  cast(shuttle as varchar) as shuttle_run
from src

