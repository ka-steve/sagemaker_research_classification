WITH
stg_filtered_work_chapters_methodology_ AS (
    SELECT * FROM "02_stg".stg_filtered_work_chapters_methodology
),
stg_filtered_work_chapters_methodology_single AS (
    SELECT
        *
    FROM
        stg_filtered_work_chapters_methodology_
    WHERE
        number_of_methodology_chapters_per_work = 1
)
SELECT * FROM stg_filtered_work_chapters_methodology_single