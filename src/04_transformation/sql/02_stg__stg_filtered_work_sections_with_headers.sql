WITH
stg_filtered_work_sections_ AS (
    SELECT 
        *
    FROM
        "02_stg".stg_filtered_work_sections
),
stg_unified_works_filtered_with_section_stats_ AS (
    SELECT 
        id_semanticscholar,
        has_sections,
        has_headers,
        has_paragraphs,
        number_of_sections,
        number_of_headers,
        number_of_paragraphs
    FROM
        "02_stg".stg_unified_works_filtered_with_section_stats
),
-- keep only those exploded sections where the work itself has headers defined
stg_filtered_work_sections_with_headers AS (
SELECT
    stg_filtered_work_sections_.*,
    stg_unified_works_filtered_with_section_stats_.has_sections AS work_has_sections,
    stg_unified_works_filtered_with_section_stats_.has_headers AS work_has_headers,
    stg_unified_works_filtered_with_section_stats_.has_paragraphs AS work_has_paragraphs,
    stg_unified_works_filtered_with_section_stats_.number_of_sections AS work_number_of_sections,
    stg_unified_works_filtered_with_section_stats_.number_of_headers AS work_number_of_headers,
    stg_unified_works_filtered_with_section_stats_.number_of_paragraphs AS work_number_of_paragraphs
FROM 
    stg_filtered_work_sections_
LEFT JOIN
    stg_unified_works_filtered_with_section_stats_
ON
    stg_filtered_work_sections_.work_id = stg_unified_works_filtered_with_section_stats_.id_semanticscholar
WHERE
    COALESCE(stg_unified_works_filtered_with_section_stats_.has_headers, 0) != 0
)
SELECT * FROM stg_filtered_work_sections_with_headers