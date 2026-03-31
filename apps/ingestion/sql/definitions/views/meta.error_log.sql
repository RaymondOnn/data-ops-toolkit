CREATE OR REPLACE VIEW META.ERROR_LOG AS
    WITH run_summaries AS (
        SELECT 
            RUN_ID,
            JOB_ID,
            -- Get the very first time it failed for the 'Error Timestamp'
            MIN(CASE WHEN JOB_STATUS = 'FAILED' THEN LAST_UPDATED_AT_TS END) as ERROR_TIMESTAMP,
            -- Get the current/latest status of this specific run
            argMax(JOB_STATUS, LAST_UPDATED_AT_TS) as LATEST_STATUS,
            -- Get the latest message or manifest from the failure
            argMax(FINAL_MANIFEST, CASE WHEN JOB_STATUS = 'FAILED' THEN LAST_UPDATED_AT_TS END) as ERROR_DETAILS
        FROM execution_log
        GROUP BY RUN_ID, JOB_ID
    )
    SELECT 
        RUN_ID,
        JOB_ID,
        DATASET_ID,
        RUN_DATE,
        ERROR_TIMESTAMP,
        ERROR_DETAILS,
        -- If the latest state is SUCCESS, it means it was recovered
        CASE 
            WHEN LATEST_STATUS = 'SUCCESS' THEN 1 
            ELSE 0 
        END AS RESOLVED,
        LATEST_STATUS
    FROM run_summaries
    -- Only include runs that have failed at least once
    WHERE ERROR_TIMESTAMP IS NOT NULL;