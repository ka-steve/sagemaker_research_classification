WITH
raw_openalex_works_reduced AS 
(
    SELECT * FROM "01_raw"."openalex_works_reduced"
),
base_openalex_works_reduced AS 
(
    SELECT
        id_openalex_short AS id_openalex,
        LOWER(id_doi_short) AS id_doi,
        title,
        "language",
        COALESCE(primary_topic_short_id, primary_topic_long_id) AS primary_topic_id,
        primary_topic_display_name,
        COALESCE(primary_topic_subfield_short_id, primary_topic_subfield_long_id) AS primary_topic_subfield_id,
        primary_topic_subfield_display_name,
        COALESCE(primary_topic_field_short_id, primary_topic_field_long_id) AS primary_topic_field_id,
        primary_topic_field_display_name,
        COALESCE(primary_topic_domain_short_id, primary_topic_domain_long_id) AS primary_topic_domain_id,
        primary_topic_domain_display_name
    FROM
        raw_openalex_works_reduced
),
base_openalex_works_reduced_filtered AS 
(
    SELECT
        *
    FROM
        base_openalex_works_reduced
    WHERE
        primary_topic_field_id='17' AND -- 'Physical Sciences'/'Computer Science'
        primary_topic_subfield_id IS NOT NULL AND
        primary_topic_id IS NOT NULL
)
SELECT * FROM base_openalex_works_reduced_filtered