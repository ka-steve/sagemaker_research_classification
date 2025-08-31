WITH
raw_semanticscholar_s2orcv2 AS 
(
    SELECT * FROM "01_raw".semanticscholar_s2orc_v2
),
base_semanticscholar_s2orcv2_step01 AS 
(
    SELECT
        corpusid AS id_semanticscholar,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.externalids.mag') AS id_mag,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.externalids.doi') AS id_doi,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.externalids.arxiv') AS id_arxiv,
        title,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.license') AS original_license,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.url') AS source_url,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.status') AS openaccess_status,
        JSON_EXTRACT_SCALAR(CAST(body AS JSON), '$.text') AS content_text,
        JSON_EXTRACT_SCALAR(CAST(body AS JSON), '$.annotations.paragraph') AS annotations_paragraph,
        JSON_EXTRACT_SCALAR(CAST(body AS JSON), '$.annotations.section_header') AS annotations_section_header
     FROM 
         raw_semanticscholar_s2orcv2
),
base_semanticscholar_s2orcv2 AS (
    SELECT
        id_semanticscholar,
        id_mag,
        id_doi,
        id_arxiv,
        title,
        source_url,
        openaccess_status,
        content_text,
        annotations_paragraph,
        annotations_section_header,
        CASE
            -- These licenses might or might not be usable just because they are openly accessible, so we are assuming they are not
            WHEN 
                original_license IN(
                    'acs-specific: authorchoice/editors choice usage agreement',
                    'elsevier-specific: oa user license',
                    'other-oa',
                    'publisher-specific, author manuscript: http://academic.oup.com/journals/pages/about_us/legal/notices',
                    'publisher-specific-oa',
                    'unspecified-oa',
                    'Open Government Licence - Canada',
                    'publisher-specific, author manuscript',
                    'implied-oa',
                    'publisher-specific, author manuscript: http://rsc.li/journals-terms-of-use',
                    'publisher-specific, author manuscript: http://onlinelibrary.wiley.com/termsAndConditions',
                    'publisher-specific, author manuscript: http://onlinelibrary.wiley.com/termsAndConditions#am',
                    'publisher-specific license'
                ) 
            THEN NULL -- 'unknown-reusability' TODO: convert NULLs to unknown-reusability after the model is joined with arxiv license information
            -- A few public domain licenses were abbreviated
            WHEN original_license = 'pd' THEN 'public-domain'
            ELSE original_license
        END AS license
     FROM 
         base_semanticscholar_s2orcv2_step01
)
SELECT * FROM base_semanticscholar_s2orcv2