WITH
stg_semanticscholar_combined_works_ AS (
    SELECT * FROM "02_stg".stg_semanticscholar_combined_works
),
stg_semanticscholar_combined_works_metadata AS (
    SELECT
        id_semanticscholar,
        id_mag,
        id_doi,
        id_arxiv,
        source_url,
        openaccess_status,
        license,
        publication_year,
        publication_date
    FROM
        stg_semanticscholar_combined_works_
)
SELECT * FROM stg_semanticscholar_combined_works_metadata