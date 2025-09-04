WITH
-- Base models refreshed with LOWER DOI
core_unified_works AS (
    SELECT * FROM "03_core".unified_works
),
topics AS (
    SELECT
        topic_index,
        topic_display_name,
        subfield_index,
        subfield_display_name
    FROM
        core_unified_works
    GROUP BY
        topic_index,
        topic_display_name,
        subfield_index,
        subfield_display_name
)
SELECT * FROM topics ORDER BY topic_index