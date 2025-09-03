WITH
stg_semanticscholar_combined_works_metadata_ AS (
    SELECT * FROM "02_stg".stg_semanticscholar_combined_works_metadata
),
base_arxiv_metadata_ AS (
    SELECT * FROM "02_stg".base_arxiv_metadata
),
stg_unified_works_01_joined_to_arxiv_step01 AS (
    SELECT
        stg_semanticscholar_combined_works_metadata_.id_semanticscholar,
        stg_semanticscholar_combined_works_metadata_.id_mag,
        stg_semanticscholar_combined_works_metadata_.id_doi,
        stg_semanticscholar_combined_works_metadata_.id_arxiv,
        stg_semanticscholar_combined_works_metadata_.source_url,
        stg_semanticscholar_combined_works_metadata_.openaccess_status,
        stg_semanticscholar_combined_works_metadata_.publication_year,
        stg_semanticscholar_combined_works_metadata_.publication_date,
        --stg_semanticscholar_combined_works_.license AS original_semanticscholar_license,
        --base_arxiv_metadata_.license AS arxiv_license,
        COALESCE(
            stg_semanticscholar_combined_works_metadata_.license,
            base_arxiv_metadata_.license,
            'unknown-reusability' -- If there's no info on it on either S2 or Arxiv, we consider it not usable 
        ) AS license
    FROM
        stg_semanticscholar_combined_works_metadata_
    LEFT JOIN
        base_arxiv_metadata_
    ON
        stg_semanticscholar_combined_works_metadata_.id_arxiv = base_arxiv_metadata_.id_arxiv
),
stg_unified_works_metadata_01_joined_to_arxiv AS (
SELECT
    *,
    CASE
        WHEN license IN ('public-domain', 'mit', 'gpl', 'CC0', 'CCBY', 'CCBYNC') THEN 1
        ELSE 0
    END AS license_allows_derivative_reuse
FROM
    stg_unified_works_01_joined_to_arxiv_step01
)
SELECT * FROM stg_unified_works_metadata_01_joined_to_arxiv