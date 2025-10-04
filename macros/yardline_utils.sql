{% macro yardline_100_num(yardline_100_col, posteam_col) %}
  /* Returns a numeric yardline_100.
     Priority: use existing numeric yardline_100 if present; otherwise parse yrdln (e.g., "SF 34").
     Rule: if yrdln team == posteam -> own yard line, so 100 - N; else opponent yard line, so N.
  */
    (
      CASE
        WHEN {{ yardline_100_col }} IS NULL OR {{ posteam_col }} IS NULL THEN NULL
        ELSE
          CASE
            WHEN SPLIT_PART({{ yardline_100_col }}, ' ', 1) = {{ posteam_col }}
              THEN 100 - TRY_CAST(SPLIT_PART({{ yardline_100_col }}, ' ', 2) AS INTEGER)
            ELSE
              TRY_CAST(SPLIT_PART({{ yardline_100_col }}, ' ', 2) AS INTEGER)
          END
      END
    )
{% endmacro %}
