WITH
stg_unified_works_metadata_03_filtered_and_tagged_ AS (
    SELECT * FROM "02_stg".stg_unified_works_metadata_03_filtered_and_tagged
),
raw_topics AS ( 
-- fix for mismatch in topic display name where some works were tagged with a topic id, and with a display name not belonging that topic id
SELECT * FROM "01_raw".openalex_topics
),
topics_grouped AS (
    SELECT 
        openalex_primary_topic_id,
        --openalex_primary_topic_display_name, has duplications
        COUNT(*) AS openalex_primary_topic_count--,
        --openalex_primary_topic_subfield_id,
        --openalex_primary_topic_subfield_display_name
    FROM
        stg_unified_works_metadata_03_filtered_and_tagged_
    GROUP BY
        openalex_primary_topic_id--,
        --openalex_primary_topic_display_name,
        --openalex_primary_topic_subfield_id,
        --openalex_primary_topic_subfield_display_name
),
numbered_topics AS (
    SELECT 
        (ROW_NUMBER() OVER(ORDER BY openalex_primary_topic_count DESC)) -1 AS openalex_primary_topic_index,
        openalex_primary_topic_id,
        --openalex_primary_topic_display_name,
        openalex_primary_topic_count,
        openalex_primary_topic_count * 100.0 / (SELECT SUM(openalex_primary_topic_count) FROM topics_grouped) openalex_primary_topic_percent--,
        --openalex_primary_topic_subfield_id,
        --openalex_primary_topic_subfield_display_name
    FROM
        topics_grouped
),
stg_topics AS (
    SELECT
        numbered_topics.openalex_primary_topic_index,
        numbered_topics.openalex_primary_topic_id,
        raw_topics.display_name AS openalex_primary_topic_display_name,
        numbered_topics.openalex_primary_topic_count,
        numbered_topics.openalex_primary_topic_percent,
        
        CAST(
            REPLACE(
                raw_topics.subfield.id, 
                'https://openalex.org/subfields/',
                ''
            )
        AS INTEGER) AS openalex_primary_topic_subfield_id,
        raw_topics.subfield.display_name AS openalex_primary_topic_subfield_display_name
    FROM
        numbered_topics
    LEFT JOIN
        raw_topics
    ON
        CONCAT('https://openalex.org/T', CAST(numbered_topics.openalex_primary_topic_id AS VARCHAR)) = raw_topics.id
)
SELECT * FROM stg_topics WHERE openalex_primary_topic_id=10036 ORDER BY openalex_primary_topic_index