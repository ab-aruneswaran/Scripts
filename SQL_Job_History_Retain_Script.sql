USE DBADB;
GO

--Archive Table
CREATE TABLE dbo.JobRunHistoryArchive
(
    ArchiveID INT IDENTITY(1,1) PRIMARY KEY,       -- Unique identifier
    JobName NVARCHAR(256) NOT NULL,                -- SQL Agent job name
    RunStatus NVARCHAR(20) NULL,                   -- Succeeded, Failed, etc.
    RunDate DATETIME NULL,                         -- Job execution date/time
    RunDuration INT NULL,                          -- Duration in seconds
    CreatedDate DATETIME NOT NULL DEFAULT GETDATE() -- Insert timestamp
);

--History Collection Script 

--The below mentioned sql script will add next step of index maintenance jobs

;WITH JobHistory AS (
    SELECT 
        j.name AS JobName,
        CASE jh.run_status
            WHEN 0 THEN 'Failed'
            WHEN 1 THEN 'Succeeded'
            WHEN 2 THEN 'Retry'
            WHEN 3 THEN 'Canceled'
            WHEN 4 THEN 'In Progress'
        END AS RunStatus,
        
        -- Convert run_date + run_time safely to DATETIME
        DATEADD(SECOND, 
            (jh.run_time % 100)                 -- seconds
            + ((jh.run_time / 100) % 100) * 60  -- minutes
            + (jh.run_time / 10000) * 3600,     -- hours
            CAST(CAST(jh.run_date AS CHAR(8)) AS DATETIME)
        ) AS RunDateTime,
        
        jh.run_duration,
        ROW_NUMBER() OVER (PARTITION BY j.job_id ORDER BY jh.run_date DESC, jh.run_time DESC) AS rn
    FROM msdb.dbo.sysjobhistory jh
    INNER JOIN msdb.dbo.sysjobs j ON jh.job_id = j.job_id
    WHERE jh.step_id = 0
)
INSERT INTO DBADB.dbo.JobRunHistoryArchive (JobName, RunStatus, RunDate, RunDuration)
SELECT 
    JobName,
    RunStatus,
    RunDateTime,
    run_duration
FROM JobHistory
WHERE rn = 1 AND JobName='DBA_CPU_Data_Collection';
