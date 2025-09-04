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
)
SELECT * FROM metadata_tagged