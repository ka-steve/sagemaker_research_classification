WITH
stg_topics_ AS (
    SELECT * FROM "02_stg".stg_topics
),
subfields_grouped AS (
    SELECT 
        openalex_primary_topic_subfield_id,
        openalex_primary_topic_subfield_display_name,
        SUM(openalex_primary_topic_count) AS openalex_primary_topic_subfield_count
    FROM
        stg_topics_
    GROUP BY
        openalex_primary_topic_subfield_id,
        openalex_primary_topic_subfield_display_name
),
stg_subfields AS (
    SELECT 
        ROW_NUMBER() OVER(ORDER BY openalex_primary_topic_subfield_count DESC) AS openalex_primary_topic_subfield_index,
        openalex_primary_topic_subfield_id,
        openalex_primary_topic_subfield_display_name,
        openalex_primary_topic_subfield_count
    FROM
        subfields_grouped
)
SELECT * FROM stg_subfields