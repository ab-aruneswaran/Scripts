DECLARE @IsRunning BIT = 0;

-- Check if the job is currently running
IF EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobactivity ja
    JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id
    WHERE j.name = N'DBA_ARCHIVAL'
      AND ja.start_execution_date IS NOT NULL
      AND ja.stop_execution_date IS NULL
)
BEGIN
    SET @IsRunning = 1;
END

-- Only stop if running AND time is within restricted window
IF @IsRunning = 1
   AND CONVERT(TIME, GETDATE()) BETWEEN '05:30:00' AND '06:00:00'
BEGIN
    PRINT 'Stopping job because it is running in restricted time window.';
    EXEC msdb.dbo.sp_stop_job @job_name = N'DBA_ARCHIVAL';
END
ELSE
BEGIN
    PRINT 'Job not running OR not in restricted time window. Skipping stop.';
END
