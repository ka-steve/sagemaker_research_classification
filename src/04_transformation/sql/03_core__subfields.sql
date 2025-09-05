WITH
stg_subfields_ AS (
    SELECT * FROM "02_stg".stg_subfields
),
subfields AS (
    SELECT
        openalex_primary_topic_subfield_index AS subfield_index,
        openalex_primary_topic_subfield_id AS subfield_original_id,
        openalex_primary_topic_subfield_display_name AS subfield_display_name,
        openalex_primary_topic_subfield_count AS subfield_count
    FROM
        stg_subfields_
)
SELECT * FROM subfields