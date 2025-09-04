WITH
stg_unified_works_metadata_03_filtered_and_tagged_ AS (
    SELECT * FROM "02_stg".stg_unified_works_metadata_03_filtered_and_tagged
),
stg_subfields_ AS (
SELECT * FROM "02_stg".stg_subfields
),
stg_topics_ AS (
SELECT * FROM "02_stg".stg_topics
),
stg_unified_works_metadata_04_with_topics_and_subfields AS (
    SELECT
        metadata_tagged.id_semanticscholar,
        metadata_tagged.id_mag,
        metadata_tagged.id_doi,
        metadata_tagged.id_arxiv,
        metadata_tagged.publication_year,
        metadata_tagged.publication_date,
        metadata_tagged.license,
        metadata_tagged.license_allows_derivative_reuse,
        metadata_tagged.source_url,
        metadata_tagged.has_id_mag,
        metadata_tagged.has_id_doi,
        metadata_tagged.has_id_mag_or_doi,
        metadata_tagged.openalex_id_openalex,
        metadata_tagged.openalex_id_doi,
        metadata_tagged.openalex_language,
    
        metadata_tagged.openalex_primary_topic_id,
        metadata_tagged.openalex_primary_topic_display_name,
        topics_indexed.openalex_primary_topic_count,
        topics_indexed.openalex_primary_topic_index,
    
        metadata_tagged.openalex_primary_topic_subfield_id,
        metadata_tagged.openalex_primary_topic_subfield_display_name,
        subfields_indexed.openalex_primary_topic_subfield_count,
        subfields_indexed.openalex_primary_topic_subfield_index,
    
        metadata_tagged.openalex_primary_topic_field_id,
        metadata_tagged.openalex_primary_topic_field_display_name,
    
        metadata_tagged.openalex_primary_topic_domain_id,
        metadata_tagged.openalex_primary_topic_domain_display_name,
    
        metadata_tagged.openalex_joined_on,

        metadata_tagged.subset
    FROM
        stg_unified_works_metadata_03_filtered_and_tagged_ AS metadata_tagged
    
    LEFT JOIN
        stg_topics_ AS topics_indexed
    ON
        metadata_tagged.openalex_primary_topic_id = topics_indexed.openalex_primary_topic_id
    
    LEFT JOIN
        stg_subfields_ AS subfields_indexed
    ON
        metadata_tagged.openalex_primary_topic_subfield_id = subfields_indexed.openalex_primary_topic_subfield_id
)
SELECT * FROM stg_unified_works_metadata_04_with_topics_and_subfields