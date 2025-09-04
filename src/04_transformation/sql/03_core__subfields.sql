WITH
-- Base models refreshed with LOWER DOI
core_unified_works AS (
    SELECT * FROM "03_core".unified_works
),
subfields AS (
    SELECT
        subfield_index,
        subfield_display_name
    FROM
        core_unified_works
    GROUP BY
        subfield_index,
        subfield_display_name
)
SELECT * FROM subfields ORDER BY subfield_index