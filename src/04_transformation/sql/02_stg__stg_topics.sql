WITH
stg_unified_works_metadata_03_filtered_and_tagged_ AS (
    SELECT * FROM "02_stg".stg_unified_works_metadata_03_filtered_and_tagged
),
topics_grouped AS (
    SELECT 
        openalex_primary_topic_id,
        openalex_primary_topic_display_name,
        COUNT(*) AS openalex_primary_topic_count,
        openalex_primary_topic_subfield_id,
        openalex_primary_topic_subfield_display_name
    FROM
        stg_unified_works_metadata_03_filtered_and_tagged_
    GROUP BY
        openalex_primary_topic_id,
        openalex_primary_topic_display_name,
        openalex_primary_topic_subfield_id,
        openalex_primary_topic_subfield_display_name
),
stg_topics AS (
    SELECT 
        (ROW_NUMBER() OVER(ORDER BY openalex_primary_topic_count DESC)) -1 AS openalex_primary_topic_index,
        openalex_primary_topic_id,
        openalex_primary_topic_display_name,
        openalex_primary_topic_count,
        openalex_primary_topic_count * 100.0 / (SELECT SUM(openalex_primary_topic_count) FROM topics_grouped) openalex_primary_topic_percent,
        openalex_primary_topic_subfield_id,
        openalex_primary_topic_subfield_display_name
    FROM
        topics_grouped
)
SELECT * FROM stg_topics ORDER BY openalex_primary_topic_index