-- models/staging/stg_win_totals.sql
{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('RAW', 'win_totals_data') }}
)

select
  cast(game_id as varchar) as game_id,
  cast(market_type as varchar) as market_type,
  cast(abbr as varchar) as team_abbr,
  cast(lines as varchar) as lines,
  cast(odds as integer) as odds,
  cast(opening_lines as integer) as opening_lines,
  cast(opening_odds as integer) as opening_odds,
  cast(book as varchar) as book,
  cast(season as integer) as season
from src

