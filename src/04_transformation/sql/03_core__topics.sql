WITH
stg_topics_ AS (
    SELECT * FROM "02_stg".stg_topics
),
topics AS (
    SELECT
        openalex_primary_topic_index AS topic_index,
        openalex_primary_topic_id AS topic_original_id,
        openalex_primary_topic_display_name AS topic_display_name,
        openalex_primary_topic_count AS topic_count,
        openalex_primary_topic_subfield_id AS subfield_original_id,
        openalex_primary_topic_subfield_display_name AS subfield_display_name
    FROM
        stg_topics_
)
SELECT * FROM topics