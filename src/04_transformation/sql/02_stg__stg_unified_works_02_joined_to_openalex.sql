WITH
-- Base models refreshed with LOWER DOI
stg_unified_works_01_joined_to_arxiv_ AS (
    SELECT * FROM "02_stg".stg_unified_works_01_joined_to_arxiv
),
base_openalex_works_reduced_ AS (
SELECT * FROM "02_stg"."base_openalex_works_reduced"
),
flagged_semanticscholar_s2orcv2 AS (
    SELECT
        *,
        CASE WHEN COALESCE(id_mag, '')='' THEN 0 ELSE 1 END AS has_id_mag,
        CASE WHEN COALESCE(id_doi, '')='' THEN 0 ELSE 1 END AS has_id_doi,
        CASE WHEN COALESCE(id_doi, '')='' AND COALESCE(id_mag, '')='' THEN 0 ELSE 1 END AS has_id_mag_or_doi
    FROM
        stg_unified_works_01_joined_to_arxiv_
),
s2oa_join_on_doi_id AS (
    SELECT
        flagged_semanticscholar_s2orcv2.id_semanticscholar,
        flagged_semanticscholar_s2orcv2.id_mag,
        flagged_semanticscholar_s2orcv2.id_doi,
        flagged_semanticscholar_s2orcv2.id_arxiv,
        flagged_semanticscholar_s2orcv2.publication_year,
        flagged_semanticscholar_s2orcv2.publication_date,
        flagged_semanticscholar_s2orcv2.license,
        flagged_semanticscholar_s2orcv2.title,
        flagged_semanticscholar_s2orcv2.content_abstract,
        flagged_semanticscholar_s2orcv2.content_text,
        flagged_semanticscholar_s2orcv2.source_url,
        flagged_semanticscholar_s2orcv2.openaccess_status,
        flagged_semanticscholar_s2orcv2.annotations_paragraph,
        flagged_semanticscholar_s2orcv2.annotations_section_header,
        flagged_semanticscholar_s2orcv2.has_id_mag,
        flagged_semanticscholar_s2orcv2.has_id_doi,
        flagged_semanticscholar_s2orcv2.has_id_mag_or_doi,
        
        base_openalex_works_reduced_.id_openalex AS openalex_doi_id_openalex,
        base_openalex_works_reduced_.id_doi AS openalex_doi_id_doi,
        base_openalex_works_reduced_.title AS openalex_doi_title,
        base_openalex_works_reduced_.language AS openalex_doi_language,
        base_openalex_works_reduced_.primary_topic_id AS openalex_doi_primary_topic_id,
        base_openalex_works_reduced_.primary_topic_display_name AS openalex_doi_primary_topic_display_name,
        base_openalex_works_reduced_.primary_topic_subfield_id AS openalex_doi_primary_topic_subfield_id,
        base_openalex_works_reduced_.primary_topic_subfield_display_name AS openalex_doi_primary_topic_subfield_display_name,
        base_openalex_works_reduced_.primary_topic_field_id AS openalex_doi_primary_topic_field_id,
        base_openalex_works_reduced_.primary_topic_field_display_name AS openalex_doi_primary_topic_field_display_name,
        base_openalex_works_reduced_.primary_topic_domain_id AS openalex_doi_primary_topic_domain_id,
        base_openalex_works_reduced_.primary_topic_domain_display_name AS openalex_doi_primary_topic_domain_display_name,
        CASE WHEN base_openalex_works_reduced_.id_doi IS NULL THEN 0 ELSE 1 END AS openalex_doi_join_worked
    FROM
        flagged_semanticscholar_s2orcv2
    LEFT JOIN
        base_openalex_works_reduced_
    ON
        flagged_semanticscholar_s2orcv2.id_doi = base_openalex_works_reduced_.id_doi
),
s2oa_join_on_mag_openalex_id AS (
    SELECT
        s2oa_join_on_doi_id.id_semanticscholar,
        s2oa_join_on_doi_id.id_mag,
        s2oa_join_on_doi_id.id_doi,
        s2oa_join_on_doi_id.id_arxiv,
        s2oa_join_on_doi_id.publication_year,
        s2oa_join_on_doi_id.publication_date,
        s2oa_join_on_doi_id.license,
        s2oa_join_on_doi_id.title,
        s2oa_join_on_doi_id.content_abstract,
        s2oa_join_on_doi_id.content_text,
        s2oa_join_on_doi_id.source_url,
        s2oa_join_on_doi_id.openaccess_status,
        s2oa_join_on_doi_id.annotations_paragraph,
        s2oa_join_on_doi_id.annotations_section_header,
        s2oa_join_on_doi_id.has_id_mag,
        s2oa_join_on_doi_id.has_id_doi,
        s2oa_join_on_doi_id.has_id_mag_or_doi,
        
        s2oa_join_on_doi_id.openalex_doi_id_openalex,
        s2oa_join_on_doi_id.openalex_doi_id_doi,
        s2oa_join_on_doi_id.openalex_doi_title,
        s2oa_join_on_doi_id.openalex_doi_language,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_id,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_display_name,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_subfield_id,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_subfield_display_name,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_field_id,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_field_display_name,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_domain_id,
        s2oa_join_on_doi_id.openalex_doi_primary_topic_domain_display_name,
        s2oa_join_on_doi_id.openalex_doi_join_worked,
        
        base_openalex_works_reduced_.id_openalex AS openalex_mag_id_openalex,
        base_openalex_works_reduced_.id_doi AS openalex_mag_id_doi,
        base_openalex_works_reduced_.title AS openalex_mag_title,
        base_openalex_works_reduced_.language AS openalex_mag_language,
        base_openalex_works_reduced_.primary_topic_id AS openalex_mag_primary_topic_id,
        base_openalex_works_reduced_.primary_topic_display_name AS openalex_mag_primary_topic_display_name,
        base_openalex_works_reduced_.primary_topic_subfield_id AS openalex_mag_primary_topic_subfield_id,
        base_openalex_works_reduced_.primary_topic_subfield_display_name AS openalex_mag_primary_topic_subfield_display_name,
        base_openalex_works_reduced_.primary_topic_field_id AS openalex_mag_primary_topic_field_id,
        base_openalex_works_reduced_.primary_topic_field_display_name AS openalex_mag_primary_topic_field_display_name,
        base_openalex_works_reduced_.primary_topic_domain_id AS openalex_mag_primary_topic_domain_id,
        base_openalex_works_reduced_.primary_topic_domain_display_name AS openalex_mag_primary_topic_domain_display_name,
        CASE WHEN base_openalex_works_reduced_.id_openalex IS NULL THEN 0 ELSE 1 END AS openalex_mag_join_worked
    FROM
        s2oa_join_on_doi_id
    LEFT JOIN
        base_openalex_works_reduced_
    ON
        s2oa_join_on_doi_id.openalex_doi_join_worked=0 AND -- only joining those through MAG that we couldn't through DOI
        s2oa_join_on_doi_id.id_mag = base_openalex_works_reduced_.id_openalex
),
stg_unified_works_02_joined_to_openalex AS (
    SELECT
        s2oa_join_on_mag_openalex_id.id_semanticscholar,
        s2oa_join_on_mag_openalex_id.id_mag,
        s2oa_join_on_mag_openalex_id.id_doi,
        s2oa_join_on_mag_openalex_id.id_arxiv,
        s2oa_join_on_mag_openalex_id.publication_year,
        s2oa_join_on_mag_openalex_id.publication_date,
        s2oa_join_on_mag_openalex_id.license,
        s2oa_join_on_mag_openalex_id.title,
        s2oa_join_on_mag_openalex_id.content_abstract,
        s2oa_join_on_mag_openalex_id.content_text,
        s2oa_join_on_mag_openalex_id.source_url,
        s2oa_join_on_mag_openalex_id.openaccess_status,
        s2oa_join_on_mag_openalex_id.annotations_paragraph,
        s2oa_join_on_mag_openalex_id.annotations_section_header,
        s2oa_join_on_mag_openalex_id.has_id_mag,
        s2oa_join_on_mag_openalex_id.has_id_doi,
        s2oa_join_on_mag_openalex_id.has_id_mag_or_doi,

        -- Unifying the two joins 
        -- not using COALESCE to make sure the selection is based on the same value every time, so either dataset A or dataset B is fully picked
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_id_openalex 
            ELSE openalex_mag_id_openalex 
        END AS openalex_id_openalex,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_id_doi 
            ELSE openalex_mag_id_doi 
        END AS openalex_id_doi,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_title 
            ELSE openalex_mag_title 
        END AS openalex_title,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_language 
            ELSE openalex_mag_language 
        END AS openalex_language,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_id 
            ELSE openalex_mag_primary_topic_id 
        END AS openalex_primary_topic_id,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_display_name 
            ELSE openalex_mag_primary_topic_display_name 
        END AS openalex_primary_topic_display_name,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_subfield_id 
            ELSE openalex_mag_primary_topic_subfield_id 
        END AS openalex_primary_topic_subfield_id,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_subfield_display_name
            ELSE openalex_mag_primary_topic_subfield_display_name
        END AS openalex_primary_topic_subfield_display_name,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_field_id
            ELSE openalex_mag_primary_topic_field_id 
        END AS openalex_primary_topic_field_id,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_field_display_name
            ELSE openalex_mag_primary_topic_field_display_name
        END AS openalex_primary_topic_field_display_name,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_domain_id
            ELSE openalex_mag_primary_topic_domain_id
        END AS openalex_primary_topic_domain_id,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN openalex_doi_primary_topic_domain_display_name
            ELSE openalex_mag_primary_topic_domain_display_name
        END AS openalex_,
        CASE 
            WHEN openalex_doi_id_doi IS NOT NULL 
            THEN 'doi'
            ELSE CASE 
                WHEN openalex_mag_id_doi IS NOT NULL 
                THEN 'mag'
                ELSE NULL
            END
        END AS openalex_joined_on
    FROM
        s2oa_join_on_mag_openalex_id
)
SELECT * FROM stg_unified_works_02_joined_to_openalex