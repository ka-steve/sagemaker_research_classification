WITH
stg_unified_works_filtered_semibalanced_reduced_ AS (
    SELECT * FROM "02_stg".stg_unified_works_filtered_semibalanced_reduced
),
topic_counts AS (
    SELECT
        openalex_primary_topic_display_name,
        COUNT(*) AS openalex_primary_topic_count
    FROM
        stg_unified_works_filtered_semibalanced_reduced_
    GROUP BY
        openalex_primary_topic_display_name
),
stg_topics_semibalanced AS (
    SELECT
        *,
        (ROW_NUMBER() OVER (ORDER BY openalex_primary_topic_count DESC) -1) AS openalex_primary_topic_index
    FROM
        topic_counts
)
SELECT * FROM stg_topics_semibalanced