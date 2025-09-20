WITH
stg_unified_works_filtered_semibalanced_reduced_ AS (
    SELECT * FROM "02_stg".stg_unified_works_filtered_semibalanced_reduced
),
stg_subfields_semibalanced_ AS (
    SELECT * FROM stg_subfields_semibalanced
),
stg_topics_semibalanced_ AS (
    SELECT * FROM stg_topics_semibalanced
),
reindexed AS (
    SELECT    
        stg_unified_works_filtered_semibalanced_reduced_.id_semanticscholar,
        stg_unified_works_filtered_semibalanced_reduced_.id_mag,
        stg_unified_works_filtered_semibalanced_reduced_.id_doi,
        stg_unified_works_filtered_semibalanced_reduced_.id_arxiv,
        stg_unified_works_filtered_semibalanced_reduced_.publication_year,
        stg_unified_works_filtered_semibalanced_reduced_.publication_date,
        stg_unified_works_filtered_semibalanced_reduced_.license,
        stg_unified_works_filtered_semibalanced_reduced_.license_allows_derivative_reuse,
        stg_unified_works_filtered_semibalanced_reduced_.source_url,
        stg_unified_works_filtered_semibalanced_reduced_.has_id_mag,
        stg_unified_works_filtered_semibalanced_reduced_.has_id_doi,
        stg_unified_works_filtered_semibalanced_reduced_.has_id_mag_or_doi,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_id_openalex,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_id_doi,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_language,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_id,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_display_name,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_count,
        stg_topics_semibalanced_.openalex_primary_topic_index,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_subfield_id,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_subfield_display_name,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_subfield_count,
        stg_subfields_semibalanced_.openalex_primary_topic_subfield_index,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_field_id,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_field_display_name,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_domain_id,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_domain_display_name,
        stg_unified_works_filtered_semibalanced_reduced_.openalex_joined_on,
        stg_unified_works_filtered_semibalanced_reduced_.title,
        stg_unified_works_filtered_semibalanced_reduced_.content_abstract,
        stg_unified_works_filtered_semibalanced_reduced_.content_text,
        stg_unified_works_filtered_semibalanced_reduced_.annotations_paragraph,
        stg_unified_works_filtered_semibalanced_reduced_.annotations_section_header
    FROM
        stg_unified_works_filtered_semibalanced_reduced_
        
    LEFT JOIN
        stg_topics_semibalanced_
    ON
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_display_name = stg_topics_semibalanced_.openalex_primary_topic_display_name
    
    LEFT JOIN
        stg_subfields_semibalanced_
    ON
        stg_unified_works_filtered_semibalanced_reduced_.openalex_primary_topic_subfield_display_name = stg_subfields_semibalanced_.openalex_primary_topic_subfield_display_name
),
reindexed_stratified AS (
    SELECT 
        *,
        NTILE(10) OVER( PARTITION BY openalex_primary_topic_display_name ORDER BY random()) AS bucket_10p
    FROM
        reindexed
),
stg_unified_works_filtered_semibalanced AS (
    SELECT 
        *,
        CASE 
            WHEN bucket_10p = 1 THEN 'test'
            WHEN bucket_10p = 2 THEN 'validation'
            ELSE 'train'
        END AS subset --80-10-10 split
    FROM
        reindexed_stratified
)
SELECT * FROM stg_unified_works_filtered_semibalanced