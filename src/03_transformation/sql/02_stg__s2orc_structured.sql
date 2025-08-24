WITH
raw_semanticscholar_s2orc AS (
    SELECT * FROM "01_raw"."semanticscholar_semanticscholar"
),
-- Extracting fields from JSON columns in the raw_semanticscholar table
extracted_fields_01 AS (
SELECT 
    corpusid,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.text') AS content_text,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.source.pdfurls') AS content_source_pdfurls,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.source.pdfsha') AS content_source_pdfsha,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.source.oainfo.license') AS content_source_oainfo_license,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.source.oainfo.openaccessurl') AS content_source_oainfo_openaccessurl,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.source.oainfo.status') AS content_source_oainfo_status,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.abstract') AS content_annotations_abstract,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.author') AS content_annotations_author,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.authoraffiliation') AS content_annotations_authoraffiliation,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.authorfirstname') AS content_annotations_authorfirstname,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.authorlastname') AS content_annotations_authorlastname,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.bibauthor') AS content_annotations_bibauthor,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.bibauthorfirstname') AS content_annotations_bibauthorfirstname,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.bibauthorlastname') AS content_annotations_bibauthorlastname,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.bibentry') AS content_annotations_bibentry,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.bibref') AS content_annotations_bibref,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.bibtitle') AS content_annotations_bibtitle,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.bibvenue') AS content_annotations_bibvenue,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.figure') AS content_annotations_figure,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.figurecaption') AS content_annotations_figurecaption,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.figureref') AS content_annotations_figureref,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.formula') AS content_annotations_formula,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.paragraph') AS content_annotations_paragraph,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.publisher') AS content_annotations_publisher,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.sectionheader') AS content_annotations_sectionheader,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.table') AS content_annotations_table,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.tableref') AS content_annotations_tableref,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.title') AS content_annotations_title,
    JSON_EXTRACT_SCALAR(CAST(content AS JSON), '$.annotations.venue') AS content_annotations_venue,
    JSON_EXTRACT_SCALAR(CAST(externalids AS JSON), '$.arxiv') AS externalids_arxiv,
    JSON_EXTRACT_SCALAR(CAST(externalids AS JSON), '$.mag') AS externalids_mag,
    JSON_EXTRACT_SCALAR(CAST(externalids AS JSON), '$.acl') AS externalids_acl,
    JSON_EXTRACT_SCALAR(CAST(externalids AS JSON), '$.pubmed') AS externalids_pubmed,
    JSON_EXTRACT_SCALAR(CAST(externalids AS JSON), '$.pubmedcentral') AS externalids_pubmedcentral,
    JSON_EXTRACT_SCALAR(CAST(externalids AS JSON), '$.dblp') AS externalids_dblp,
    JSON_EXTRACT_SCALAR(CAST(externalids AS JSON), '$.doi') AS externalids_doi
FROM 
    raw_semanticscholar_s2orc
),
-- Final selection and type casting
s2orc_structured AS (
SELECT 
    corpusid,
    SUBSTR(
        content_text, 
        CAST(JSON_EXTRACT_SCALAR(content_annotations_title, '$[0].start') AS INT), 
        CAST(JSON_EXTRACT_SCALAR(content_annotations_title, '$[0].end') AS INT) - CAST(JSON_EXTRACT_SCALAR(content_annotations_title, '$[0].start') AS INT) + 1
    ) AS content_title,
    content_text,
    content_source_pdfurls,
    content_source_pdfsha,
    content_source_oainfo_license,
    content_source_oainfo_openaccessurl,
    content_source_oainfo_status,
    content_annotations_abstract,
    content_annotations_author,
    content_annotations_authoraffiliation,
    content_annotations_authorfirstname,
    content_annotations_authorlastname,
    content_annotations_bibauthor,
    content_annotations_bibauthorfirstname,
    content_annotations_bibauthorlastname,
    content_annotations_bibentry,
    content_annotations_bibref,
    content_annotations_bibtitle,
    content_annotations_bibvenue,
    content_annotations_figure,
    content_annotations_figurecaption,
    content_annotations_figureref,
    content_annotations_formula,
    content_annotations_paragraph,
    content_annotations_publisher,
    content_annotations_sectionheader,
    content_annotations_table,
    content_annotations_tableref,
    content_annotations_title,
    content_annotations_venue,
    externalids_arxiv,
    externalids_mag,
    externalids_acl,
    externalids_pubmed,
    externalids_pubmedcentral,
    externalids_dblp,
    externalids_doi
FROM 
    extracted_fields_01
)
SELECT * FROM s2orc_structured
LIMIT 10