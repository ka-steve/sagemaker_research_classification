WITH
core_unified_works_semibalanced AS (
    SELECT * FROM "03_core".unified_works_semibalanced
),
core_unified_works_semibalanced_subset AS (
    SELECT
        *
    FROM
        core_unified_works_semibalanced
    WHERE
        subset = 'validation'
)
SELECT * FROM core_unified_works_semibalanced_subset