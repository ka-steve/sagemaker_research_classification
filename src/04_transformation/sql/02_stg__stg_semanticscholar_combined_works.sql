WITH
base_semanticscholar_s2orcv2_ AS (
    SELECT * FROM "02_stg".base_semanticscholar_s2orcv2
),
raw_semanticscholar_papers AS (
    SELECT * FROM "01_raw".semanticscholar_papers
),
raw_semanticscholar_abstracts AS (
    SELECT * FROM "01_raw".semanticscholar_abstracts
),
stg_semanticscholar_combined_works AS (
    SELECT
        base_semanticscholar_s2orcv2_.id_semanticscholar,
        base_semanticscholar_s2orcv2_.id_mag,
        base_semanticscholar_s2orcv2_.id_doi,
        base_semanticscholar_s2orcv2_.id_arxiv,
        base_semanticscholar_s2orcv2_.title,
        base_semanticscholar_s2orcv2_.source_url,
        base_semanticscholar_s2orcv2_.openaccess_status,
        base_semanticscholar_s2orcv2_.content_text,
        base_semanticscholar_s2orcv2_.annotations_paragraph,
        base_semanticscholar_s2orcv2_.annotations_section_header,
        base_semanticscholar_s2orcv2_.license,

        --CASE WHEN raw_semanticscholar_abstracts.corpusid IS NULL THEN 0 ELSE 1 END AS abstracts_join_worked,
        raw_semanticscholar_abstracts.abstract AS content_abstract,

        --CASE WHEN raw_semanticscholar_papers.corpusid IS NULL THEN 0 ELSE 1 END AS papers_join_worked,
        raw_semanticscholar_papers.year AS publication_year,
        raw_semanticscholar_papers.publicationdate AS publication_date
    FROM
        base_semanticscholar_s2orcv2_
    LEFT JOIN
        raw_semanticscholar_papers
    ON
        base_semanticscholar_s2orcv2_.id_semanticscholar = raw_semanticscholar_papers.corpusid
    LEFT JOIN
        raw_semanticscholar_abstracts
    ON
        base_semanticscholar_s2orcv2_.id_semanticscholar = raw_semanticscholar_abstracts.corpusid
)
SELECT * FROM stg_semanticscholar_combined_works