-- macros/k_fantasy_points.sql
{% macro k_fantasy_points(
    fgm_0_39,
    fgm_40_49,
    fgm_50_plus,
    xpm,
    xpa
) %}

{# Default scoring can be overridden via vars.k_scoring #}
{% set scoring = var('k_scoring', {
  'fg_0_39': 3,
  'fg_40_49': 4,
  'fg_50_plus': 5,
  'fg_miss': -1,
  'xp_made': 1,
  'xp_miss': -1
}) %}

(
  ({{ fgm_0_39 }}    * {{ scoring['fg_0_39'] }}) +
  ({{ fgm_40_49 }}   * {{ scoring['fg_40_49'] }}) +
  ({{ fgm_50_plus }} * {{ scoring['fg_50_plus'] }}) +
  (((COALESCE({{ fgm_0_39 }},0)+COALESCE({{ fgm_40_49 }},0)+COALESCE({{ fgm_50_plus }},0))) * {{ scoring['fg_miss'] }}) +
  ({{ xpm }} * {{ scoring['xp_made'] }}) +
  ((COALESCE({{ xpa }},0)-COALESCE({{ xpm }},0)) * {{ scoring['xp_miss'] }})
)
{% endmacro %}