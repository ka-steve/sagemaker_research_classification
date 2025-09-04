WITH
-- Base models refreshed with LOWER DOI
core_unified_works AS (
    SELECT * FROM "03_core".unified_works
),
core_unified_works_subset AS (
    SELECT
        *
    FROM
        core_unified_works
    WHERE
        subset = 'validation'
)
SELECT * FROM core_unified_works_subset