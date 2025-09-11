-- models/staging/stg_injuries.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'injuries_data') }}
)

select
  cast(season as integer) as season,
  cast(game_type as varchar) as game_type,
  cast(team as varchar) as team,
  cast(week as integer) as week,
  cast(gsis_id as varchar) as gsis_id,
  cast(position as varchar) as position,
  cast(full_name as varchar) as full_name,
  cast(first_name as varchar) as first_name,
  cast(last_name as varchar) as last_name,
  cast(report_primary_injury as varchar) as report_primary_injury,
  cast(report_secondary_injury as varchar) as report_secondary_injury,
  cast(report_status as varchar) as report_status,
  cast(practice_primary_injury as varchar) as practice_primary_injury,
  cast(practice_secondary_injury as varchar) as practice_secondary_injury,
  cast(practice_status as varchar) as practice_status,
  cast(date_modified as timestamp_ntz) as date_modified
from src

