WITH
stg_unified_works_filtered_ AS (
    SELECT * FROM "02_stg".stg_unified_works_filtered
),
stg_filtered_work_sections_ AS (
SELECT * FROM "02_stg".stg_filtered_work_sections
),
max_header_index_per_work AS (
    SELECT 
        work_id,
        MAX(section_type_index) AS max_header_index
    FROM
        stg_filtered_work_sections_ 
    WHERE 
        section_type='header'
    GROUP BY
        work_id
),
max_paragraph_index_per_work AS (
    SELECT 
        work_id,
        MAX(section_type_index) AS max_paragraph_index
    FROM
        stg_filtered_work_sections_ 
    WHERE 
        section_type='paragraph'
    GROUP BY
        work_id
),
max_section_index_per_work AS (
    SELECT 
        work_id,
        MAX(section_index) AS max_section_index
    FROM
        stg_filtered_work_sections_ 
    GROUP BY
        work_id
),
stg_unified_works_filtered_with_section_stats AS (
    SELECT
        stg_unified_works_filtered_.*,
        COALESCE(max_section_index_per_work.max_section_index, 0) AS number_of_sections,
        COALESCE(max_header_index_per_work.max_header_index, 0) AS number_of_headers,
        COALESCE(max_paragraph_index_per_work.max_paragraph_index, 0) AS number_of_paragraphs,
        CASE COALESCE(max_section_index_per_work.max_section_index, 0)
            WHEN 0 THEN 0
            ELSE 1
        END AS has_sections,
        CASE COALESCE(max_header_index_per_work.max_header_index, 0)
            WHEN 0 THEN 0
            ELSE 1
        END AS has_headers,
        CASE COALESCE(max_paragraph_index_per_work.max_paragraph_index, 0)
            WHEN 0 THEN 0
            ELSE 1
        END AS has_paragraphs
    FROM
        stg_unified_works_filtered_
        
    LEFT JOIN
        max_section_index_per_work
    ON
        stg_unified_works_filtered_.id_semanticscholar = max_section_index_per_work.work_id
        
    LEFT JOIN
        max_header_index_per_work
    ON
        stg_unified_works_filtered_.id_semanticscholar = max_header_index_per_work.work_id
        
    LEFT JOIN
        max_paragraph_index_per_work
    ON
        stg_unified_works_filtered_.id_semanticscholar = max_paragraph_index_per_work.work_id
)
SELECT 
        *
FROM
    stg_unified_works_filtered_with_section_stats