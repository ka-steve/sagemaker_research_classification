WITH
stg_unified_works_filtered_ AS (
    SELECT * FROM "02_stg".stg_unified_works_filtered
),
works_numbered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY openalex_primary_topic_subfield_display_name ORDER BY RANDOM()) AS subfield_row_number,
        ROW_NUMBER() OVER (PARTITION BY openalex_primary_topic_display_name ORDER BY RANDOM()) AS topic_row_number
    FROM
        stg_unified_works_filtered_
),
stratified_sampling AS (
    SELECT
        *
    FROM
        works_numbered
    WHERE
        subfield_row_number < 20000 AND -- elbow-logic 
        topic_row_number < 2500 -- elbow-logic 
),
counted_by_topic AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY openalex_primary_topic_display_name) AS new_topic_count
    FROM
        stratified_sampling
),
stg_unified_works_filtered_semibalanced_reduced AS (
    SELECT
        *
    FROM
        counted_by_topic
    WHERE
        new_topic_count > 400 -- ~0.1% cutoff rate
)
SELECT * FROM stg_unified_works_filtered_semibalanced_reduced