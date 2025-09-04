WITH
stg_unified_works_metadata_02_joined_to_openalex_ AS (
    SELECT * FROM "02_stg".stg_unified_works_metadata_02_joined_to_openalex
),
stg_semanticscholar_combined_works_content_ AS (
SELECT * FROM "02_stg".stg_semanticscholar_combined_works_content
),
metadata_filtered AS (
    SELECT 
        * 
    FROM
        stg_unified_works_metadata_02_joined_to_openalex_
    WHERE
        openalex_language='en' AND
        license_allows_derivative_reuse=1
),
metadata_stratified AS (
    SELECT 
        *,
        NTILE(10) OVER( PARTITION BY openalex_primary_topic_id ORDER BY random()) AS bucket_10p
    FROM
        metadata_filtered
    WHERE
        openalex_language='en' AND
        license_allows_derivative_reuse=1
),
metadata_tagged AS (
    SELECT 
        *,
        CASE 
            WHEN bucket_10p = 1 THEN 'test'
            WHEN bucket_10p = 2 THEN 'validation'
            ELSE 'train'
        END AS subset --80-10-10 split
    FROM
        metadata_stratified
    WHERE
        openalex_language='en' AND
        license_allows_derivative_reuse=1
),
subfields_grouped AS (
    SELECT 
        openalex_primary_topic_subfield_id,
        COUNT(*) AS openalex_primary_topic_subfield_count
    FROM
        metadata_tagged
    GROUP BY
        openalex_primary_topic_subfield_id
),
subfields_indexed AS (
    SELECT 
        openalex_primary_topic_subfield_id,
        openalex_primary_topic_subfield_count,
        ROW_NUMBER() OVER(ORDER BY openalex_primary_topic_subfield_count DESC) AS openalex_primary_topic_subfield_index
    FROM
        subfields_grouped
    ORDER BY
        openalex_primary_topic_subfield_count DESC
),
topics_grouped AS (
    SELECT 
        openalex_primary_topic_id,
        COUNT(*) AS openalex_primary_topic_count
    FROM
        metadata_tagged
    GROUP BY
        openalex_primary_topic_id
),
topics_indexed AS (
    SELECT 
        openalex_primary_topic_id,
        openalex_primary_topic_count,
        ROW_NUMBER() OVER(ORDER BY openalex_primary_topic_count DESC) AS openalex_primary_topic_index
    FROM
        topics_grouped
    ORDER BY
        openalex_primary_topic_count DESC
),
stg_unified_works_filtered AS (
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
    
        stg_semanticscholar_combined_works_content_.title,
        stg_semanticscholar_combined_works_content_.content_abstract,
        stg_semanticscholar_combined_works_content_.content_text,
        stg_semanticscholar_combined_works_content_.annotations_paragraph,
        stg_semanticscholar_combined_works_content_.annotations_section_header,

        metadata_tagged.subset
    FROM
        metadata_tagged
    
    LEFT JOIN
        topics_indexed
    ON
        metadata_tagged.openalex_primary_topic_id = topics_indexed.openalex_primary_topic_id
    
    LEFT JOIN
        subfields_indexed
    ON
        metadata_tagged.openalex_primary_topic_subfield_id = subfields_indexed.openalex_primary_topic_subfield_id
    
    LEFT JOIN
        stg_semanticscholar_combined_works_content_
    ON
        metadata_tagged.id_semanticscholar = stg_semanticscholar_combined_works_content_.id_semanticscholar
)
SELECT * FROM stg_unified_works_filtered