-- models/staging/stg_contracts.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'contracts_data') }}
)

select
  cast(player as varchar) as player,
  cast(position as varchar) as position,
  cast(team as varchar) as team,
  cast(is_active as boolean) as is_active,
  cast(year_signed as integer) as year_signed,
  cast(years as integer) as years,
  cast(value as float) as value,
  cast(apy as float) as apy,
  cast(guaranteed as float) as guaranteed,
  cast(apy_cap_pct as float) as apy_cap_pct,
  cast(inflated_value as float) as inflated_value,
  cast(inflated_apy as float) as inflated_apy,
  cast(inflated_guaranteed as float) as inflated_guaranteed,
  cast(player_page as varchar) as player_page,
  cast(otc_id as integer) as otc_id,
  cast(gsis_id as varchar) as gsis_id,
  cast(date_of_birth as varchar) as date_of_birth,
  cast(height as varchar) as height,
  cast(weight as integer) as weight,
  cast(college as varchar) as college,
  cast(draft_year as integer) as draft_year,
  cast(draft_round as varchar) as draft_round,
  cast(draft_overall as varchar) as draft_overall,
  cast(draft_team as varchar) as draft_team,
  cast(cols as varchar) as cols
from src