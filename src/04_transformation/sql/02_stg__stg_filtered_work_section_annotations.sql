WITH
stg_unified_works_filtered_ AS (
    SELECT 
        id_semanticscholar,
        annotations_section_header,
        annotations_paragraph
    FROM
        "02_stg"."stg_unified_works_filtered"
    --WHERE
        --id_semanticscholar=236504355
    --LIMIT 3
),
extracted_header_positions AS (
SELECT
    id_semanticscholar AS work_id,
    JSON_EXTRACT_SCALAR(section, '$.attributes.n') AS n,
    CAST(JSON_EXTRACT_SCALAR(section, '$.start') AS INTEGER) AS section_start,
    CAST(JSON_EXTRACT_SCALAR(section, '$.end') AS INTEGER) AS section_end,
    'header' AS section_type
FROM
    stg_unified_works_filtered_
CROSS JOIN
    UNNEST(
        CAST(
            JSON_PARSE(annotations_section_header)AS ARRAY(JSON)
        )
    ) AS T(section)
),
numbered_header_positions AS (
    SELECT
        work_id,
        ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY section_start, section_end) AS section_type_index,
        n,
        section_start,
        section_end,
        section_type
    FROM
        extracted_header_positions
),
extracted_paragraph_positions AS (
SELECT
    id_semanticscholar AS work_id,
    CAST(NULL AS VARCHAR) AS n,
    CAST(JSON_EXTRACT_SCALAR(section, '$.start') AS INTEGER) AS section_start,
    CAST(JSON_EXTRACT_SCALAR(section, '$.end') AS INTEGER) AS section_end,
    'paragraph' AS section_type
FROM
    stg_unified_works_filtered_
CROSS JOIN
    UNNEST(
        CAST(
            JSON_PARSE(annotations_paragraph)AS ARRAY(JSON)
        )
    ) AS T(section)
),  
numbered_paragraph_positions AS (
    SELECT
        work_id,
        ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY section_start, section_end) AS section_type_index,
        n,
        section_start,
        section_end,
        section_type
    FROM
        extracted_paragraph_positions
),
numbered_section_positions_unordered AS (
SELECT * FROM numbered_header_positions UNION ALL
SELECT * FROM numbered_paragraph_positions
),
stg_filtered_work_section_annotations AS (
    SELECT 
        work_id,
        ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY section_start, section_end) AS section_index,
        section_type_index,
        n,
        section_start,
        section_end,
        section_type
    FROM
        numbered_section_positions_unordered
    ORDER BY
        work_id, section_start
)
SELECT * FROM stg_filtered_work_section_annotations