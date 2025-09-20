WITH
stg_topics_semibalanced_ AS (
    SELECT * FROM "02_stg".stg_topics_semibalanced
),
topics_semibalanced AS (
    SELECT
        openalex_primary_topic_index AS topic_index,
        openalex_primary_topic_display_name AS topic_display_name,
        openalex_primary_topic_count AS topic_count
    FROM
        stg_topics_semibalanced_
)
SELECT * FROM topics_semibalanced