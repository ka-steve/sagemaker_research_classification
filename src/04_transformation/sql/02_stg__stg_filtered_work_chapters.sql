WITH
stg_filtered_work_sections_with_headers_ AS (
    SELECT * FROM "02_stg".stg_filtered_work_sections_with_headers-- WHERE work_id IN (236504355, 226307095, 258436829)
    --SELECT * FROM "02_stg".stg_filtered_work_sections_with_headers LIMIT 100
),
sections_with_header_info AS (
    SELECT 
        work_id,
        section_index,
        section_type_index,
        CASE
            WHEN section_type='header' THEN section_type_index
            ELSE CAST(NULL AS INT) 
        END AS header_header_index,
        section_start,
        section_end,
        section_type,
        CASE
            WHEN section_type='header' THEN section_text
            ELSE CAST(NULL AS VARCHAR) 
        END AS header_text,
        CASE
            WHEN section_type='header' THEN section_text_length
            ELSE CAST(NULL AS INT) 
        END AS header_text_length,
        section_text,
        section_text_length
    FROM
        stg_filtered_work_sections_with_headers_
),
sections_with_header_index AS (
SELECT 
    work_id,
    section_index,
    section_type_index,
    header_header_index,
    COALESCE(
        header_header_index,
        LAG(header_header_index) IGNORE NULLS OVER (PARTITION BY work_id ORDER BY section_index),
        0 -- in case we have paragraphs preceding the first header
    ) AS header_index,
    section_start,
    section_end,
    section_type,
    header_text,
    header_text_length,
    section_text,
    section_text_length
FROM
    sections_with_header_info
),
stg_filtered_work_chapters AS (
    SELECT
        work_id,
        ARRAY_AGG(section_index) AS section_indices,
        ARRAY_AGG(section_type_index) AS section_type_indices,
        header_index,
        COUNT(section_text) AS number_of_sections_in_block,
        ARRAY_AGG(section_start) AS section_starts,
        MIN(section_start) AS min_section_start,
        ARRAY_AGG(section_end) AS section_ends,
        MAX(section_end) AS max_section_end,
        LISTAGG(header_text, '') WITHIN GROUP (ORDER BY section_index) AS header_text,
        SUM(header_text_length) AS header_text_length,
        REPLACE(
            LISTAGG(section_text,  '\n\n') WITHIN GROUP (ORDER BY section_index),
            '\n',
            CHR(10)
        ) AS block_text,
        SUM(section_text_length) + COUNT(section_text)*2 -2 AS block_text_length --accomodating for extra line breaks
    FROM
        sections_with_header_index
    GROUP BY
        work_id,
        header_index
)
SELECT * FROM stg_filtered_work_chapters ORDER BY work_id, min_section_start