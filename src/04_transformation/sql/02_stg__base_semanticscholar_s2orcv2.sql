WITH
raw_semanticscholar_s2orcv2 AS 
(
    SELECT * FROM "01_raw".semanticscholar_s2orc_v2
),
base_semanticscholar_s2orcv2 AS 
(
    SELECT
        corpusid AS id_semanticscholar,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.externalids.mag') AS id_mag,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.externalids.doi') AS id_doi,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.externalids.arxiv') AS id_arxiv,
        title,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.license') AS license,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.url') AS source_url,
        JSON_EXTRACT_SCALAR(CAST(openaccessinfo AS JSON), '$.status') AS openaccess_status,
        JSON_EXTRACT_SCALAR(CAST(body AS JSON), '$.text') AS content_text,
        JSON_EXTRACT_SCALAR(CAST(body AS JSON), '$.annotations.paragraph') AS annotations_paragraph,
        JSON_EXTRACT_SCALAR(CAST(body AS JSON), '$.annotations.section_header') AS annotations_section_header
     FROM 
         raw_semanticscholar_s2orcv2
)
SELECT * FROM base_semanticscholar_s2orcv2