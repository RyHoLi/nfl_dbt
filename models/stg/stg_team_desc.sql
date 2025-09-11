-- models/staging/stg_team_desc.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'team_desc_data') }}
)

select
  cast(team_abbr as varchar) as team_abbr,
  cast(team_name as varchar) as team_name,
  cast(team_id as integer) as team_id,
  cast(team_nick as varchar) as team_nick,
  cast(team_conf as varchar) as team_conference,
  cast(team_division as varchar) as team_division,
  cast(team_color as varchar) as team_color,
  cast(team_color2 as varchar) as team_color2,
  cast(team_color3 as varchar) as team_color3,
  cast(team_color4 as varchar) as team_color4,
  cast(team_logo_wikipedia as varchar) as team_logo_wikipedia,
  cast(team_logo_espn as varchar) as team_logo_espn,
  cast(team_wordmark as varchar) as team_wordmark,
  cast(team_conference_logo as varchar) as team_conference_logo,
  cast(team_league_logo as varchar) as team_league_logo,
  cast(team_logo_squared as varchar) as team_logo_squared
from src

