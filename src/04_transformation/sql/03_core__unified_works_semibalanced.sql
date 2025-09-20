WITH
-- Base models refreshed with LOWER DOI
stg_unified_works_filtered_semibalanced_ AS (
    SELECT * FROM "02_stg".stg_unified_works_filtered_semibalanced
),
core_unified_works_semibalanced AS (
    SELECT
        id_semanticscholar AS id,
        openalex_primary_topic_index AS topic_index,
        openalex_primary_topic_display_name AS topic_display_name,
        openalex_primary_topic_subfield_index AS subfield_index,
        openalex_primary_topic_subfield_display_name AS subfield_display_name,
        title,
        content_abstract AS abstract,
        content_text AS fulltext,
    
        subset
    FROM
        stg_unified_works_filtered_semibalanced_
)
SELECT * FROM core_unified_works_semibalanced