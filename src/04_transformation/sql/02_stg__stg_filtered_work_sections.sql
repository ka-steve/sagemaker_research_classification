WITH
stg_unified_works_filtered_ AS (
    SELECT 
        id_semanticscholar,
        content_text
    FROM
        "02_stg"."stg_unified_works_filtered"
    --WHERE
        --id_semanticscholar=236504355
    --LIMIT 3
),
stg_filtered_work_section_annotations_ AS (
    SELECT 
        *
    FROM
        "02_stg".stg_filtered_work_section_annotations
),
stg_filtered_work_sections_with_text AS (
SELECT
    work_id,
    section_index,
    section_type_index,
    n,
    section_start,
    section_end,
    section_type,
    SUBSTR(stg_unified_works_filtered_.content_text, "section_start" +1, "section_end" - "section_start") AS section_text
FROM 
    stg_filtered_work_section_annotations_
LEFT JOIN
    stg_unified_works_filtered_
ON
    stg_filtered_work_section_annotations_.work_id = stg_unified_works_filtered_.id_semanticscholar
),
stg_filtered_work_sections AS (
    SELECT
        *,
        LENGTH(section_text) AS section_text_length
    FROM
        stg_filtered_work_sections_with_text
)
SELECT * FROM stg_filtered_work_sections