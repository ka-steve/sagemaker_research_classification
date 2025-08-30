WITH
raw_arxiv_metadata AS 
(
    SELECT * FROM "01_raw".arxiv_metadatada -- mind the typo, fixed in downstream models
),
base_arxiv_metadata AS 
(
    SELECT
        id AS id_arxiv, 
        doi AS id_doi,
        title,
        abstract,
        -- Converting license definitions to the same format as SemanticScholar
        CASE
            WHEN "license" IS NULL THEN NULL
            WHEN "license" = 'http://creativecommons.org/licenses/by-nc-sa/4.0/' THEN 'CCBYNCSA'
            WHEN "license" = 'http://creativecommons.org/publicdomain/zero/1.0/' THEN 'CC0'
            WHEN "license" = 'http://creativecommons.org/licenses/by-nc-nd/4.0/' THEN 'CCBYNCND'
            WHEN "license" = 'http://creativecommons.org/licenses/by/4.0/' THEN 'CCBY'
            WHEN "license" = 'http://creativecommons.org/licenses/by-sa/4.0/' THEN 'CCBYSA'
            WHEN "license" = 'http://creativecommons.org/licenses/by/3.0/' THEN 'CCBY'
            WHEN "license" = 'http://creativecommons.org/licenses/by-nc-sa/3.0/' THEN 'CCBYNCSA'
            WHEN "license" = 'http://creativecommons.org/licenses/publicdomain/' THEN 'public-domain'
            WHEN "license" = 'http://arxiv.org/licenses/nonexclusive-distrib/1.0/' THEN 'ArXiv nonexclusive-distrib'
            ELSE CONCAT('ArXiv: ', "license")
        END AS license
    FROM
        raw_arxiv_metadata
)
SELECT * FROM base_arxiv_metadata