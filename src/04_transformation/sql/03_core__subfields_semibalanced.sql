WITH
stg_subfields_semibalanced_ AS (
    SELECT * FROM "02_stg".stg_subfields_semibalanced
),
subfields_semibalanced AS (
    SELECT
        openalex_primary_topic_subfield_index AS subfield_index,
        openalex_primary_topic_subfield_display_name AS subfield_display_name,
        openalex_primary_topic_subfield_count AS subfield_count
    FROM
        stg_subfields_semibalanced_
)
SELECT * FROM subfields_semibalanced