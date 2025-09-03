WITH
stg_semanticscholar_combined_works_ AS (
    SELECT * FROM "02_stg".stg_semanticscholar_combined_works
),
stg_semanticscholar_combined_works_content AS (
    SELECT
        id_semanticscholar,
        title,
        content_abstract,
        content_text,
        annotations_paragraph,
        annotations_section_header
    FROM
        stg_semanticscholar_combined_works_
)
SELECT * FROM stg_semanticscholar_combined_works_content