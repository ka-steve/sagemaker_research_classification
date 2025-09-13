WITH
stg_filtered_work_chapters_ AS (
    SELECT * FROM "02_stg".stg_filtered_work_chapters
    --SELECT * FROM "02_stg".stg_filtered_work_sections_with_headers LIMIT 100
),
stg_filtered_work_chapters_methodology AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY work_id) number_of_methodology_chapters_per_work
    FROM
        stg_filtered_work_chapters_
    WHERE
        chapter_is_research_methodology > 0
)
SELECT * FROM stg_filtered_work_chapters_methodology