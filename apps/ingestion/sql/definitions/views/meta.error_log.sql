DROP VIEW IF EXISTS META.ERROR_LOG;
CREATE VIEW META.ERROR_LOG AS
WITH run_summaries AS (
    SELECT
        RUN_ID,
        JOB_ID,
        DATASET_ID, -- Added to the CTE selection
        RUN_DATE,   -- Added to the CTE selection
        min(multiIf(JOB_STATUS = 'FAILED', LAST_UPDATED_AT_TS, NULL)) AS ERROR_TIMESTAMP,
        argMax(JOB_STATUS, LAST_UPDATED_AT_TS) AS LATEST_STATUS,
        argMax(FINAL_MANIFEST, multiIf(JOB_STATUS = 'FAILED', LAST_UPDATED_AT_TS, NULL)) AS ERROR_DETAILS
    FROM META.EXECUTION_LOG
    -- Include all identifying columns in the GROUP BY to make them available in the scope
    GROUP BY 
        RUN_ID, 
        JOB_ID, 
        DATASET_ID, 
        RUN_DATE
)
SELECT
    RUN_ID,
    JOB_ID,
    DATASET_ID,
    RUN_DATE,
    ERROR_TIMESTAMP,
    ERROR_DETAILS,
    multiIf(LATEST_STATUS = 'SUCCESS', 1, 0) AS RESOLVED,
    LATEST_STATUS
FROM run_summaries
WHERE ERROR_TIMESTAMP IS NOT NULL;
