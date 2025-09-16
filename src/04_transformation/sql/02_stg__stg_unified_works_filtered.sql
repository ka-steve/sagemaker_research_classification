WITH
stg_unified_works_metadata_04_with_topics_and_subfields_ AS (
    SELECT * FROM "02_stg".stg_unified_works_metadata_04_with_topics_and_subfields
),
stg_semanticscholar_combined_works_content_ AS (
SELECT * FROM "02_stg".stg_semanticscholar_combined_works_content
),
joined_metadata_and_content AS (
    SELECT
        metadata.id_semanticscholar,
        metadata.id_mag,
        metadata.id_doi,
        metadata.id_arxiv,
        metadata.publication_year,
        metadata.publication_date,
        metadata.license,
        metadata.license_allows_derivative_reuse,
        metadata.source_url,
        metadata.has_id_mag,
        metadata.has_id_doi,
        metadata.has_id_mag_or_doi,
        metadata.openalex_id_openalex,
        metadata.openalex_id_doi,
        metadata.openalex_language,
    
        metadata.openalex_primary_topic_id,
        metadata.openalex_primary_topic_display_name,
        metadata.openalex_primary_topic_count,
        metadata.openalex_primary_topic_index,
    
        metadata.openalex_primary_topic_subfield_id,
        metadata.openalex_primary_topic_subfield_display_name,
        metadata.openalex_primary_topic_subfield_count,
        metadata.openalex_primary_topic_subfield_index,
    
        metadata.openalex_primary_topic_field_id,
        metadata.openalex_primary_topic_field_display_name,
    
        metadata.openalex_primary_topic_domain_id,
        metadata.openalex_primary_topic_domain_display_name,
    
        metadata.openalex_joined_on,
    
        stg_semanticscholar_combined_works_content_.title,
        stg_semanticscholar_combined_works_content_.content_abstract,
        stg_semanticscholar_combined_works_content_.content_text,
        stg_semanticscholar_combined_works_content_.annotations_paragraph,
        stg_semanticscholar_combined_works_content_.annotations_section_header,

        metadata.subset
    FROM
        stg_unified_works_metadata_04_with_topics_and_subfields_ AS metadata
    
    LEFT JOIN
        stg_semanticscholar_combined_works_content_
    ON
        metadata.id_semanticscholar = stg_semanticscholar_combined_works_content_.id_semanticscholar
),
stg_unified_works_filtered AS (
    SELECT
        *
    FROM
        joined_metadata_and_content
    WHERE
        title IS NOT NULL AND
        content_abstract IS NOT NULL AND
        content_text IS NOT NULL
)
SELECT * FROM stg_unified_works_filtered