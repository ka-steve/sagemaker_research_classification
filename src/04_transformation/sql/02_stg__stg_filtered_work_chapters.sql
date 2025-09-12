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
        CASE
            WHEN section_type='paragraph' THEN section_text
            ELSE CAST(NULL AS VARCHAR) 
        END AS paragraph_text,
        CASE
            WHEN section_type='paragraph' THEN section_text_length
            ELSE CAST(NULL AS INT) 
        END AS paragraph_text_length--,
        --section_text,
        --section_text_length
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
    paragraph_text,
    paragraph_text_length
FROM
    sections_with_header_info
),
chapters AS (
    SELECT
        work_id,
        ARRAY_AGG(section_index) AS section_indices,
        ARRAY_AGG(section_type_index) AS section_type_indices,
        header_index,
        COUNT(paragraph_text) AS number_of_paragraphs_in_chapter,
        ARRAY_AGG(section_start) AS section_starts,
        MIN(section_start) AS min_section_start,
        ARRAY_AGG(section_end) AS section_ends,
        MAX(section_end) AS max_section_end,
        LISTAGG(header_text, '') WITHIN GROUP (ORDER BY section_index) AS header_text,
        SUM(header_text_length) AS header_text_length,
        REPLACE(
            LISTAGG(paragraph_text,  '\n\n') WITHIN GROUP (ORDER BY section_index),
            '\n',
            CHR(10)
        ) AS chapter_text,
        SUM(paragraph_text_length) + COUNT(paragraph_text)*2 -2 AS chapter_text_length --accomodating for extra line breaks
    FROM
        sections_with_header_index
    GROUP BY
        work_id,
        header_index
),
chapters_enhanced AS (
    SELECT
        work_id,
        header_index,
        number_of_paragraphs_in_chapter,
        min_section_start,
        max_section_end,
        header_text,
        LOWER(header_text) AS header_text_lower,
        header_text_length,
        chapter_text,
        chapter_text_length,
        section_starts,
        section_ends,
        section_indices,
        section_type_indices
    FROM
        chapters
),
stg_filtered_work_chapters AS (
    SELECT
        *,
                CASE
            -- Not enough text, skipping
            WHEN chapter_text_length IS NULL OR chapter_text_length < 100 THEN -1
        
             -- Data quality issues in papers, skipping
            WHEN header_text_lower IN('methodology and behavioral biometrics model', 'new methodology for bathing') THEN -2
            
             -- Not or not necesseraly what we are looking for, skipping
            WHEN header_text_lower IN('training methodology', 'methodology participants', 'benchmarking methodology', 'assessment methodology', 'optimization methodology', 'validation methodology') THEN -3
            WHEN header_text_lower LIKE '%evaluation%' THEN -4
            
            
            WHEN header_text_lower IN(
                'methodology', 'experiment methodology', 'methodology and data', 'design methodology', 'analysis methodology', 'proposed methodology', 'data and methodology', 'the proposed methodology', 'methodology overview', 'materials and methodology', 'study methodology', 'methodology of the study', 'survey methodology', 'methodology of research', 'methodology and approach', 'simulation methodology', 'methodology description', 'methodology and materials',  'material and methodology', 'methodology design', 'methodology and design', 'methodology and implementation', 'methodology and experiments', 'research and methodology',  'background and methodology', 'methodology and experimental setup', 'general methodology', 'design and methodology', 'experimental setup and methodology', 'system methodology', 'methodology and procedures', 'research questions and methodology', 'methodology and methods', 'methodology of the research', 'working methodology', 'proposed system methodology', 'dataset and methodology', 'theory and methodology', 'setup and methodology', 'methodology . overview of our network', 'overall methodology', 'problem formulation and methodology',  'data & methodology', 'solution methodology', 'description of the methodology', 'methodology and data collection', 'methodology and analysis', 'methodology and architecture', 'methodology preliminaries', 'methodology and research design', 'methodology and setup', '. . . methodology', 'classification methodology', 'proposed system and methodology', 'methodology and research methods', 'methodology and data analysis', 'methodology data collection', 'methodology dataset', 'methodology and process', 'statistical methodology', 'empirical methodology', 'data collection and methodology', 'overview of methodology', 'user study methodology', 'experimental methodology.', 'methodology and procedure',  'methods and methodology', 'motivation and methodology', 'methodology and framework', 'investigation methodology', 'methodology pipeline', 'datasets and methodology',  'methodology and system design', 'methodology details', 'proposal methodology', 'proposed methodology:', 'modeling methodology', 'systematic review methodology',  'model and methodology', 'project methodology', 'literature review methodology', 'review methodology', 'methodology research design', 'research design and methodology',  'methodology and results', 'attack methodology', 'methodology problem formulation', 'tools and methodology', 'systematic literature review methodology',  'methodology and main results', 'implementation methodology', 'methodology of analysis', 'methodology/materials', 'methodology and dataset',  'methodology and experimental results', 'methodology summary', 'approach and methodology', 'methodology of experiments', 'literature review and methodology',  'problem statement and methodology', 'programming methodology', 'study methodology:', 'applied methodology', 'proposed design methodology', 'methodology and tools',  'objective and methodology', 'development methodology', 'computational methodology', 'the methodology of the study', 'data description and methodology', 'methodology and model', 'methodology adopted', 'methodology implementation', 'methodology of the proposed system', 'methodology and model specification', 'methodology datasets', 'methodology of simulation', 'scope and methodology', 'methodology and architectures', 'proof methodology', 'methodology and theoretical framework', 'the study methodology', 'architecture and methodology',  'research objectives and methodology', 'methodology problem definition', 'methodology research method and design', 'methodology and scope', 'workflow and methodology',  'concept and working methodology', 'objectives and methodology', 'methodology and experimentation', 'methodology and instruments', 'methodology and research questions',  'methodology perspective'
            ) THEN 1
            WHEN header_text_lower LIKE '%research methodology%' THEN 2
            WHEN header_text_lower LIKE 'methodology:%' THEN 3
            WHEN header_text_lower LIKE '%: methodology' THEN 4
            WHEN header_text_lower LIKE '%proposed methodology%' THEN 5
            WHEN header_text_lower LIKE '%experimental methodology%' THEN 6
            
            WHEN header_text_lower LIKE '%methodology%' AND header_text_length<=16 THEN 7 -- max plus 5 chars like "A. ", "1) " etc
            ELSE NULL
        END AS chapter_is_research_methodology
    FROM
        chapters_enhanced
)
SELECT * FROM stg_filtered_work_chapters