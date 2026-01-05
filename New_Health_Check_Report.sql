USE [DBADB]
GO

/****** Object:  StoredProcedure [dbo].[SQLhealthcheck_report_new2]    Script Date: 1/5/2026 4:55:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



 
/**************************/  
/***** SQL SERVER HEALTH CHECK REPORT - HTML ********/
/**************************/  
-- Tested: SQL Server 2008 R2, 2012, 2014, 2016, 2017, 2019 and 2022  
-- Report Type: HTML Report Delivers to Mail Box  
-- Parameters: DBMail Profile Name *, Email ID *, Server Name (Optional);   
-- Reports: SQL Server Instance Details
--   AG Status
--   Log Shipping 
--	 Mirroring DB 
--   Instance Last Recycle Information 
--   CPU Usage 
--   PLE Usage
--   Disk Space Usage
--   Tempdb File Usage
--   User DB File Usage
--   Backup Report for User DB 
--   Wait Statistics Report
--   Failed Jobs in Last 24Hrs
--   Long Running Queries Summary
--	 Blocked Queries Summary

/**************************/  
/**************************/  
CREATE   PROCEDURE [dbo].[SQLhealthcheck_report_new2] (  
  @MailProfile NVARCHAR(50),   
  @MailID NVARCHAR(2000),  
  @Server VARCHAR(max) = NULL,
  @ClientName VARCHAR(max) = NULL)
AS  
BEGIN  
SET NOCOUNT ON;  
SET ARITHABORT ON;  
  
DECLARE @ServerName VARCHAR(max);  
SET @ServerName = ISNULL(@Server,@@SERVERNAME);  
    
/*********************/
/****** Server Reboot Details ********/
/*********************/

IF OBJECT_ID('tempdb..#RebootDetails') IS NOT NULL
    DROP TABLE #RebootDetails;

CREATE TABLE #RebootDetails
(
    ServiceName     VARCHAR(100),
    ServiceStatus   VARCHAR(50),
    RestartTime     VARCHAR(25),
    CurrentTime     VARCHAR(25),
    UpTimeInDays    INT
);

;WITH AgentStartTime AS
(
    SELECT MAX(agent_start_date) AS AgentLastStartTime
    FROM msdb.dbo.syssessions
)
INSERT INTO #RebootDetails
SELECT  
    s.servicename,
    s.status_desc,
    FORMAT(s.last_startup_time, 'yyyy-MM-dd hh:mmtt') AS RestartTime,
    FORMAT(GETDATE(), 'yyyy-MM-dd hh:mmtt')           AS CurrentTime,
    DATEDIFF(DAY, s.last_startup_time, GETDATE())     AS UpTimeInDays
FROM sys.dm_server_services s
WHERE s.servicename LIKE '%SQL Server (%'

UNION ALL

SELECT
    s.servicename,
    s.status_desc,
    FORMAT(ast.AgentLastStartTime, 'yyyy-MM-dd hh:mmtt') AS RestartTime,
    FORMAT(GETDATE(), 'yyyy-MM-dd hh:mmtt')              AS CurrentTime,
    CASE 
        WHEN ast.AgentLastStartTime IS NULL THEN NULL
        ELSE DATEDIFF(DAY, ast.AgentLastStartTime, GETDATE())
    END AS UpTimeInDays
FROM sys.dm_server_services s
CROSS JOIN AgentStartTime ast
WHERE s.servicename LIKE '%SQL Server Agent%';

/*********************/  
/***** Windows Disk Space Details ******/  
/*********************/  
 
 IF OBJECT_ID('tempdb..#output') IS NOT NULL DROP TABLE #output;
IF OBJECT_ID('tempdb..#driveinfo') IS NOT NULL DROP TABLE #driveinfo;
CREATE TABLE #driveinfo (
    Drive_Letter VARCHAR(5),
    TotalSpace_GB    DECIMAL(10,2),
    UsedSpace_GB    DECIMAL(10,2),
    FreeSpace_GB  DECIMAL(10,2),
    Percentage_Full DECIMAL(5,2)
);
declare @svrName varchar(255)
declare @sql varchar(400)
set @svrName = @@SERVERNAME
set @sql = 'powershell.exe -c "Get-WmiObject -ComputerName ' + QUOTENAME(@svrName,'''') + '-Class Win32_Volume -Filter ''DriveType = 3'' | select name,capacity,freespace | foreach{$_.name+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''}"'
CREATE TABLE #output
(line varchar(255))
insert #output
EXEC xp_cmdshell @sql
insert into #driveinfo
select rtrim(ltrim(SUBSTRING(line,1,CHARINDEX('|',line) -1))) as drivename
   ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
   (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float)/1024,0) as 'capacity(GB)'
   ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
   (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float)/1024,0) - round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1,
   (CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float) /1024 ,0) as 'usedspace(GB)'
   ,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1,
   (CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float) /1024 ,0)as 'freespace(GB)'
   ,
   
   cast(
   (round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
   (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float),0) - 
   round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1,
   (CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float),0)) /
   round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1,
   (CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float),0)
    as decimal(18,2)
	)*100 as 'Used %'
from #output where line like '[A-Z][:]%'
order by drivename

/*********************/
/***** SQL Server CPU Usage Summary (Last 24 Hours) ******/
/*********************/
IF OBJECT_ID('tempdb..#CPU') IS NOT NULL
    DROP TABLE #CPU;

CREATE TABLE #CPU
(
    servername         SYSNAME,
    Max_Total_CPU      INT,
    Min_Total_CPU      INT,
    Avg_Total_CPU      INT,
    Max_CPU_Hit_Count  INT,
    load_date          DATETIME
);

;WITH CPUStats AS
(
    SELECT
        SQLUtilisedCPU + Otherprosses AS TotalCPU
    FROM DBADB.dbo.CPUUtilisationdata
    WHERE Time >= DATEADD(HOUR, -24, GETDATE())
),
MaxCPU AS
(
    SELECT MAX(TotalCPU) AS MaxCPUValue
    FROM CPUStats
)
INSERT INTO #CPU
(
    servername,
    Max_Total_CPU,
    Min_Total_CPU,
    Avg_Total_CPU,
    Max_CPU_Hit_Count,
    load_date
)
SELECT
    @@SERVERNAME                                    AS servername,
    MAX(cs.TotalCPU)                               AS Max_Total_CPU,
    MIN(cs.TotalCPU)                               AS Min_Total_CPU,
    AVG(cs.TotalCPU)                               AS Avg_Total_CPU,
    SUM(CASE WHEN cs.TotalCPU = m.MaxCPUValue 
             THEN 1 ELSE 0 END)                    AS Max_CPU_Hit_Count,
    GETDATE()                                      AS load_date
FROM CPUStats cs
CROSS JOIN MaxCPU m;



/*********************/  
/***** SQL Server Memory Usage Details *****/  
/*********************/  
IF OBJECT_ID('tempdb..#PLE') IS NOT NULL
    DROP TABLE #PLE;

CREATE TABLE #PLE
(
    ServerName                 SYSNAME,
    MinPLE                     BIGINT,
    MaxPLE                     BIGINT,
    AvgPLE                     DECIMAL(18,2),
    PLE_Less_Than_300_Count    INT,
    LoadDate                   DATETIME
);

;WITH PLEData AS
(
    SELECT 
        CAST(PageLifeExpectancy AS BIGINT) AS PLE
    FROM DBADB.dbo.PageLifeExpectancyLog
    WHERE LoggedAt >= DATEADD(HOUR, -24, GETDATE())
)
INSERT INTO #PLE
(
    servername,
    MinPLE,
    MaxPLE,
    AvgPLE,
    PLE_Less_Than_300_Count,
    LoadDate
)
SELECT
    @@SERVERNAME                                       AS ServerName,
    MIN(PLE)                                          AS MinPLE,
    MAX(PLE)                                          AS MaxPLE,
    AVG(PLE)                                    AS AvgPLE,
    SUM(CASE WHEN PLE < 300 THEN 1 ELSE 0 END)        AS PLE_Less_Than_300_Count,
    GETDATE()                                         AS LoadDate
FROM PLEData;

/****************************************************
 AG Status Report
****************************************************/
--IF OBJECT_ID('tempdb..#AG_DB_Status') IS NOT NULL
--    DROP TABLE #AG_DB_Status;

--CREATE TABLE #AG_DB_Status
--(
--    AG_Name                     SYSNAME,
--    DatabaseName                SYSNAME,
--    PrimaryServer               SYSNAME,
--    SecondaryServer             SYSNAME,
--    AvailabilityMode            VARCHAR(50),
--    FailoverMode                VARCHAR(50),
--    SynchronizationState        VARCHAR(50),
--    SynchronizationHealth       VARCHAR(50),
--    SuspendReason               VARCHAR(100),
--    LogSendQueue_KB             BIGINT,
--    LogSendRate_KB_per_sec      BIGINT,
--    RedoQueue_KB                BIGINT,
--    RedoRate_KB_per_sec         BIGINT,
--    SecondaryLag_Seconds        BIGINT
--);

--INSERT INTO #AG_DB_Status
--(
--    AG_Name,
--    DatabaseName,
--    PrimaryServer,
--    SecondaryServer,
--    AvailabilityMode,
--    FailoverMode,
--    SynchronizationState,
--    SynchronizationHealth,
--    SuspendReason,
--    LogSendQueue_KB,
--    LogSendRate_KB_per_sec,
--    RedoQueue_KB,
--    RedoRate_KB_per_sec,
--    SecondaryLag_Seconds
--)
--SELECT
--    ag.name                                         AS AG_Name,
--    adc.database_name                               AS DatabaseName,
--    pri.replica_server_name                         AS PrimaryServer,
--    sec.replica_server_name                         AS SecondaryServer,
--    ar.availability_mode_desc                       AS AvailabilityMode,
--    ar.failover_mode_desc                           AS FailoverMode,
--    drs.synchronization_state_desc                  AS SynchronizationState,
--    drs.synchronization_health_desc                 AS SynchronizationHealth,
--    drs.suspend_reason_desc                         AS SuspendReason,
--    drs.log_send_queue_size                         AS LogSendQueue_KB,
--    drs.log_send_rate                               AS LogSendRate_KB_per_sec,
--    drs.redo_queue_size                             AS RedoQueue_KB,
--    drs.redo_rate                                   AS RedoRate_KB_per_sec,
--    drs.secondary_lag_seconds                       AS SecondaryLag_Seconds
--FROM sys.availability_groups ag
--JOIN sys.availability_replicas ar
--    ON ag.group_id = ar.group_id
--JOIN sys.dm_hadr_database_replica_states drs
--    ON ar.replica_id = drs.replica_id
--JOIN sys.availability_databases_cluster adc
--    ON drs.group_database_id = adc.group_database_id
--LEFT JOIN sys.availability_replicas pri
--    ON ag.group_id = pri.group_id
--   AND pri.replica_id = drs.replica_id
--LEFT JOIN sys.availability_replicas sec
--    ON ag.group_id = sec.group_id
--   AND sec.replica_id = drs.replica_id
--   AND drs.is_primary_replica = 0;


/****************************************************
 Log Shipping Report
****************************************************/

--IF OBJECT_ID('tempdb..#LogShippingStatus') IS NOT NULL
--    DROP TABLE #LogShippingStatus;

--CREATE TABLE #LogShippingStatus
--(
--    PrimaryServer          SYSNAME,
--    PrimaryDatabase        SYSNAME,
--    BackupDirectory        NVARCHAR(MAX),
--    BackupShare            NVARCHAR(MAX),
--    LastBackupFile         NVARCHAR(MAX),
--    LastBackupDate         VARCHAR(MAX),
--    BackupCompression      BIT,

--    SecondaryServer        SYSNAME,
--    SecondaryDatabase      SYSNAME,
--    LastCopiedFile         NVARCHAR(MAX),
--    LastCopiedDate         VARCHAR(MAX),
--    LastRestoredFile       NVARCHAR(MAX),
--    LastRestoredDate       VARCHAR(MAX),
--    LastRestoredLatency    INT,

--    HealthStatus           VARCHAR(50)
--);
--INSERT INTO #LogShippingStatus
--(
--    PrimaryServer,
--    PrimaryDatabase,
--    BackupDirectory,
--    BackupShare,
--    LastBackupFile,
--    LastBackupDate,
--    BackupCompression,
--    SecondaryServer,
--    SecondaryDatabase,
--    LastCopiedFile,
--    LastCopiedDate,
--    LastRestoredFile,
--    LastRestoredDate,
--    LastRestoredLatency,
--    HealthStatus
--)
--SELECT
--    lsm.primary_server,
--    lsp.primary_database,
--    lsp.backup_directory,
--    lsp.backup_share,
--    lsp.last_backup_file,
--    lsp.last_backup_date,
--    lsp.backup_compression,
--    lsm.secondary_server,
--    lsm.secondary_database,
--    lsm.last_copied_file,
--    lsm.last_copied_date,
--    lsm.last_restored_file,
--    lsm.last_restored_date,
--    lsm.last_restored_latency,

--    /* Health Status Calculation */
--CASE
--        WHEN lsm.last_restored_date IS NULL
--            THEN 'Restore Not Happening'

--        WHEN lsm.last_restored_latency > 60 -- according to client 
--            THEN 'Restore Lagging (>60 mins)'

--        ELSE 'Healthy'
--    END AS HealthStatus

--FROM msdb.dbo.log_shipping_primary_databases lsp
--JOIN [GEOLAPTOP-35\TESTING].msdb.dbo.log_shipping_secondary_databases lss 
--    ON lsp.primary_database = lss.secondary_database
--JOIN [GEOLAPTOP-35\TESTING].msdb.dbo.log_shipping_monitor_secondary lsm 
--    ON lss.secondary_id = lsm.secondary_id
--WHERE EXISTS
--(
--    SELECT 1
--    FROM sys.databases d
--    WHERE d.name = lsp.primary_database
--);
----SELECT *
----FROM #LogShippingStatus
----ORDER BY HealthStatus DESC, PrimaryDatabase;

/****************************************************
Mirroring Report
****************************************************/

--IF OBJECT_ID('tempdb..#DBMirroringStatus') IS NOT NULL
--    DROP TABLE #DBMirroringStatus;

--CREATE TABLE #DBMirroringStatus
--(
--    DatabaseName            SYSNAME,
--    Role                    VARCHAR(50),
--    State                   VARCHAR(50),
--    MirrorServer            SYSNAME,
--    MirrorServerInstance    SYSNAME,
--    SafetyLevel             VARCHAR(50)
--);

--INSERT INTO #DBMirroringStatus
--(
--    DatabaseName,
--    Role,
--    State,
--    MirrorServer,
--    MirrorServerInstance,
--    SafetyLevel
--)
--SELECT
--    d.name                          AS DatabaseName,
--    dm.mirroring_role_desc          AS Role,
--    dm.mirroring_state_desc         AS State,
--    dm.mirroring_partner_name       AS MirrorServer,
--    dm.mirroring_partner_instance   AS MirrorServerInstance,
--    dm.mirroring_safety_level_desc  AS SafetyLevel
--FROM sys.database_mirroring dm
--JOIN sys.databases d
--    ON dm.database_id = d.database_id
--WHERE dm.mirroring_guid IS NOT NULL;

/*********************/  
/****** Tempdb File Info *********/  
/*********************/  
-- tempdb file usage  
IF OBJECT_ID('tempdb..#tempdbfileusage') IS NOT NULL
    DROP TABLE #tempdbfileusage;

CREATE TABLE #tempdbfileusage
(
    servername        VARCHAR(MAX),
    databasename      VARCHAR(MAX),
    filename          VARCHAR(MAX),
    physicalName      VARCHAR(MAX),
    filesizeGB        DECIMAL(18,2),
    availableSpaceGB  DECIMAL(18,2),
    percentfull       DECIMAL(5,2),
    diskTotalGB       DECIMAL(18,2),
    diskFreeGB        DECIMAL(18,2),
    AutoGrowth        VARCHAR(10),
    AutoGrowthSetting VARCHAR(20),
    MaxSize           VARCHAR(20)
);

DECLARE @TEMPDBSQL NVARCHAR(MAX);

SET @TEMPDBSQL = '
Use tempdb
SELECT
    @@SERVERNAME AS servername,
    ''tempdb'' AS databasename,
    mf.name AS filename,
    mf.physical_name AS physicalName,

    CAST(mf.size * 8.0 / 1024 / 1024 AS DECIMAL(18,2)) AS filesizeGB,
    CAST((mf.size - FILEPROPERTY(mf.name, ''SpaceUsed'')) * 8.0 / 1024 / 1024 AS DECIMAL(18,2)) AS availableSpaceGB,
    CAST(FILEPROPERTY(mf.name, ''SpaceUsed'') * 100.0 / mf.size AS DECIMAL(5,2)) AS percentfull,
    CAST(vs.total_bytes / 1024.0 / 1024 / 1024 AS DECIMAL(18,2)) AS diskTotalGB,
    CAST(vs.available_bytes / 1024.0 / 1024 / 1024 AS DECIMAL(18,2)) AS diskFreeGB,

    CASE 
        WHEN mf.growth = 0 THEN ''OFF''
        ELSE ''ON''
    END AS AutoGrowth,

   CASE 
    WHEN mf.is_percent_growth = 1 
        THEN CAST(mf.growth AS VARCHAR(10)) + ''%''
    ELSE FORMAT(mf.growth * 8 / 1024.0, ''N0'') + '' MB''
END AS AutoGrowthSetting,

CASE 
    WHEN mf.max_size = -1 THEN ''UNLIMITED''
    ELSE FORMAT(mf.max_size * 8.0 / 1024 / 1024, ''N2'') + '' GB''
END AS MaxSize


FROM tempdb.sys.database_files mf
CROSS APPLY sys.dm_os_volume_stats(DB_ID(''tempdb''), mf.file_id) vs;
';

-- Insert data into temp table
INSERT INTO #tempdbfileusage
EXEC sp_executesql @TEMPDBSQL;


/*********************/  
/****** User DB File Info *********/  
/*********************/  
IF OBJECT_ID('tempdb..#UserDBFileUsage') IS NOT NULL
    DROP TABLE #UserDBFileUsage;

CREATE TABLE #UserDBFileUsage
(
    servername        VARCHAR(MAX),
    databasename      VARCHAR(MAX),
    filename          VARCHAR(MAX),
    filetype          VARCHAR(10),
    physicalName      VARCHAR(MAX),
    filesizeGB        DECIMAL(18,2),
    availableSpaceGB  DECIMAL(18,2),
    percentfull       DECIMAL(5,2),
    diskTotalGB       DECIMAL(18,2),
    diskFreeGB        DECIMAL(18,2),
    AutoGrowth        VARCHAR(10),
    AutoGrowthSetting VARCHAR(20),
    MaxSize           VARCHAR(20)
);

DECLARE @SQL1 NVARCHAR(MAX) = N'';
DECLARE @dbname SYSNAME;

DECLARE db_cursor CURSOR FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND state_desc = 'ONLINE';

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @dbname;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL1 += '
	USE ' + QUOTENAME(@dbname) + ';
    INSERT INTO #UserDBFileUsage
    SELECT
        @@SERVERNAME AS servername,
        ''' + @dbname + ''' AS databasename,
        mf.name AS filename,
        CASE mf.type WHEN 0 THEN ''DATA'' WHEN 1 THEN ''LOG'' END AS filetype,
        mf.physical_name AS physicalName,

        CAST(mf.size * 8.0 / 1024 / 1024 AS DECIMAL(18,2)) AS filesizeGB,

        CAST((mf.size - FILEPROPERTY(mf.name, ''SpaceUsed'')) * 8.0 / 1024 / 1024 AS DECIMAL(18,2)) AS availableSpaceGB,

        CAST(FILEPROPERTY(mf.name, ''SpaceUsed'') * 100.0 / mf.size AS DECIMAL(5,2)) AS percentfull,

        CAST(vs.total_bytes / 1024.0 / 1024 / 1024 AS DECIMAL(18,2)) AS diskTotalGB,
        CAST(vs.available_bytes / 1024.0 / 1024 / 1024 AS DECIMAL(18,2)) AS diskFreeGB,

        CASE WHEN mf.growth = 0 THEN ''OFF'' ELSE ''ON'' END AS AutoGrowth,

        CASE 
            WHEN mf.is_percent_growth = 1 THEN CAST(mf.growth AS VARCHAR(10)) + ''%''
            ELSE FORMAT(mf.growth * 8 / 1024.0, ''N0'') + '' MB''
        END AS AutoGrowthSetting,

        CASE 
            WHEN mf.max_size = -1 THEN ''UNLIMITED''
            ELSE FORMAT(mf.max_size * 8.0 / 1024 / 1024, ''N2'') + '' GB''
        END AS MaxSize

    FROM sys.master_files mf
    CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) vs
    WHERE mf.database_id = DB_ID(''' + @dbname + ''');
    ';

    FETCH NEXT FROM db_cursor INTO @dbname;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

EXEC sys.sp_executesql @SQL1;

/****************************************************
 Combined Backup Status Report (NO DATE CONDITION)
****************************************************/

IF OBJECT_ID('tempdb..#Backup_Report') IS NOT NULL
    DROP TABLE #Backup_Report;

CREATE TABLE #Backup_Report
(
    DatabaseName           SYSNAME,

    LastFullBackupTime     DATETIME,
    FullBackupSize_GB      DECIMAL(18,2),

    LastDiffBackupTime     DATETIME,
    DiffBackupSize_GB      DECIMAL(18,2),

    LastLogBackupTime      DATETIME,
    LogBackupSize_MB       DECIMAL(18,2)
);

;WITH BackupCTE AS
(
    SELECT
        bs.database_name,
        bs.type,
        bs.backup_finish_date,
        CAST(bs.compressed_backup_size / 1024.0 / 1024.0 / 1024.0 AS DECIMAL(18,2)) AS BackupSize_GB,
        ROW_NUMBER() OVER
        (
            PARTITION BY bs.database_name, bs.type
            ORDER BY bs.backup_finish_date DESC
        ) AS rn
    FROM msdb.dbo.backupset bs
    INNER JOIN msdb.dbo.backupmediaset bms
        ON bs.media_set_id = bms.media_set_id
)
INSERT INTO #Backup_Report
(
    DatabaseName,
    LastFullBackupTime,
    FullBackupSize_GB,
    LastDiffBackupTime,
    DiffBackupSize_GB,
    LastLogBackupTime,
    LogBackupSize_MB
)
SELECT
    d.name AS DatabaseName,

    fb.backup_finish_date AS LastFullBackupTime,
    fb.BackupSize_GB      AS FullBackupSize_GB,

    db.backup_finish_date AS LastDiffBackupTime,
    db.BackupSize_GB      AS DiffBackupSize_GB,

    lb.backup_finish_date AS LastLogBackupTime,
    lb.BackupSize_GB*1024      AS LogBackupSize_MB

FROM sys.databases d
LEFT JOIN BackupCTE fb
    ON d.name = fb.database_name
   AND fb.type = 'D'
   AND fb.rn = 1

LEFT JOIN BackupCTE db
    ON d.name = db.database_name
   AND db.type = 'I'
   AND db.rn = 1

LEFT JOIN BackupCTE lb
    ON d.name = lb.database_name
   AND lb.type = 'L'
   AND lb.rn = 1

WHERE d.database_id > 4 and DB_NAME(d.database_id) NOT IN ('DBADB')
ORDER BY d.name;

/*********************/  
/****** Job Failure Last 24 hours *********/  
/*********************/  

IF OBJECT_ID('tempdb..#JobFailureSummary') IS NOT NULL
    DROP TABLE #JobFailureSummary;

CREATE TABLE #JobFailureSummary
(
    JobName                NVARCHAR(255),
    FailureCount_Last24Hrs INT,
    FirstFailureTime_UTC   NVARCHAR(255),
    LastFailureTime_UTC    NVARCHAR(255),
    LastFailedStep         NVARCHAR(255),
    JobOwner               NVARCHAR(255),
    JobCategory            NVARCHAR(255),
    JobStatus              NVARCHAR(20)
);

INSERT INTO #JobFailureSummary
(
    JobName,
    FailureCount_Last24Hrs,
    FirstFailureTime_UTC,
    LastFailureTime_UTC,
    LastFailedStep,
    JobOwner,
    JobCategory,
    JobStatus
)
SELECT
    j.sql_server_agent_job_name AS JobName,
    COUNT(f.sql_server_agent_job_failure_id) AS FailureCount_Last24Hrs,
    FORMAT(MIN(f.job_failure_time_utc), 'yyyy-MM-dd hh:mm tt') AS FirstFailureTime_UTC,
    FORMAT(MAX(f.job_failure_time_utc), 'yyyy-MM-dd hh:mm tt') AS LastFailureTime_UTC,
    MAX(f.job_failure_step_name) AS LastFailedStep,
    MAX(j.owner_login_name) AS JobOwner,
    MAX(j.job_category_name) AS JobCategory,
    CASE 
        WHEN MAX(CAST(j.is_enabled AS INT)) = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS JobStatus
FROM DBADB.dbo.sql_server_agent_job_failure f
JOIN DBADB.dbo.sql_server_agent_job j
    ON f.sql_server_agent_job_id = j.sql_server_agent_job_id
WHERE f.job_failure_time_utc >= DATEADD(HOUR, -24, GETUTCDATE())
  AND j.is_deleted = 0
GROUP BY
    j.sql_server_agent_job_name
ORDER BY
    FailureCount_Last24Hrs DESC,
    LastFailureTime_UTC DESC;

/*********************/  
/****** Hourly Wait Stats Last 24 Hours *********/  
/*********************/  

-- Drop temp table if exists
IF OBJECT_ID('tempdb..#WaitStatsHourly') IS NOT NULL
    DROP TABLE #WaitStatsHourly;

-- Create temp table
CREATE TABLE #WaitStatsHourly
(
    DateHour    DATETIME,
    CPU_Pct     DECIMAL(10,2),
    IO_Data_Pct DECIMAL(10,2),
    IO_Log_Pct  DECIMAL(10,2),
    Memory_Pct  DECIMAL(10,2),
    Lock_Pct    DECIMAL(10,2),
	Network_Pct     DECIMAL(10,2) 
);

-- Insert aggregated hourly wait stats
;WITH Last24H AS
(
    SELECT
        DATEADD(HOUR, DATEDIFF(HOUR, 0, Date), 0) AS DateHour,
        WaitType,
        Percentage
    FROM dbo.WaitStatData
    WHERE Date >= DATEADD(HOUR,-24,GETDATE())
      AND WaitType NOT IN ('SOS_WORK_DISPATCHER')  -- mandatory exclusion
)
INSERT INTO #WaitStatsHourly (DateHour, CPU_Pct, IO_Data_Pct, IO_Log_Pct, Memory_Pct, Lock_Pct, Network_Pct)
SELECT
    DateHour,

    -- CPU
    SUM(CASE WHEN WaitType = 'SOS_SCHEDULER_YIELD'
             THEN Percentage ELSE 0 END) AS CPU_Pct,

    -- IO (Data)
    SUM(CASE WHEN WaitType LIKE 'PAGEIOLATCH%'
             THEN Percentage ELSE 0 END) AS IO_Data_Pct,

    -- IO (Log)
    SUM(CASE WHEN WaitType = 'WRITELOG'
             THEN Percentage ELSE 0 END) AS IO_Log_Pct,

    -- Memory
    SUM(CASE WHEN WaitType = 'RESOURCE_SEMAPHORE'
             THEN Percentage ELSE 0 END) AS Memory_Pct,

    -- Locking
    SUM(CASE WHEN WaitType LIKE 'LCK_%'
             THEN Percentage ELSE 0 END) AS Lock_Pct,
	-- Network
    SUM(CASE WHEN WaitType = 'ASYNC_NETWORK_IO'
             THEN Percentage ELSE 0 END) AS Network_Pct

FROM Last24H
GROUP BY DateHour
ORDER BY DateHour;

/*********************/  
/****** Long Running Queries Summary Last 24 Hours *********/  
/*********************/  

-- Drop temp table if exists
IF OBJECT_ID('tempdb..#LongRunningSummary') IS NOT NULL
    DROP TABLE #LongRunningSummary;

-- Create temp table
CREATE TABLE #LongRunningSummary
(
    Less_5_Min        INT,
    Between_5_10_Min  INT,
    Between_10_25_Min INT,
    Greater_25_Min    INT,
    TotalQueries      INT,
    SummaryDate       DATETIME DEFAULT GETDATE()
);

-- Insert aggregated counts into temp table
;WITH DistinctQueries AS
(
    -- Remove duplicates: same query text + database, pick only first occurrence
    SELECT 
        InstanceName,
        StartTime,
        SPID,
        UserName,
        ProgramName,
        DatabaseName,
        ExecutingSQL,
        WaitType,
        logdate,
        StatementText,
        StoredProcedure,
        is_closed,
        -- Convert HH:MM:SS string to total seconds
        DATEDIFF(SECOND, 0, CAST(ElapsedTime AS TIME)) AS ElapsedSeconds,
        ROW_NUMBER() OVER(
            PARTITION BY DatabaseName, ExecutingSQL 
            ORDER BY logdate
        ) AS rn
    FROM DBADB.dbo.longqrydetails
    WHERE logdate >= DATEADD(HOUR,-24,GETDATE())  -- only last 24 hours
)
INSERT INTO #LongRunningSummary (Less_5_Min, Between_5_10_Min, Between_10_25_Min, Greater_25_Min, TotalQueries)
SELECT
    COUNT(CASE WHEN ElapsedSeconds < 300 THEN 1 END) AS Less_5_Min,
    COUNT(CASE WHEN ElapsedSeconds >= 300 AND ElapsedSeconds < 600 THEN 1 END) AS Between_5_10_Min,
    COUNT(CASE WHEN ElapsedSeconds >= 600 AND ElapsedSeconds < 1500 THEN 1 END) AS Between_10_25_Min,
    COUNT(CASE WHEN ElapsedSeconds >= 1500 THEN 1 END) AS Greater_25_Min,
    COUNT(*) AS TotalQueries
FROM DistinctQueries
WHERE rn = 1;  -- only first occurrence per query

/*********************/  
/****** Head Blocker Queries Info *********/  
/*********************/  
IF OBJECT_ID('tempdb..#HeadBlockerSummary') IS NOT NULL
    DROP TABLE #HeadBlockerSummary;

CREATE TABLE #HeadBlockerSummary
(
    head_blocker_session_id   INT,
    head_blocker_query        NVARCHAR(500),
    blocking_queries_count    INT,
    BlockingDuration_HHMMSS   VARCHAR(20),
    BlockingDuration_Minutes  INT,
    logdate                   DATETIME,
    Status                    VARCHAR(20)
);
;WITH DedupHeadBlockers AS
(
    SELECT
        head_blocker_session_id,
        LEFT(head_blocker_query, 500) AS head_blocker_query,
        blocking_queries_count,
        duration,
        DATEDIFF(MINUTE, 0, CAST(duration AS TIME)) AS BlockingDuration_Minutes,
        logdate,
        ROW_NUMBER() OVER
        (
            PARTITION BY head_blocker_session_id
            ORDER BY logdate DESC
        ) AS rn
    FROM DBADB.dbo.HeadBlockingInfo
    WHERE logdate >= DATEADD(HOUR,-24,GETDATE()) --CAST(logdate AS DATE) = '2025-12-09'
    
    -- 🚫 Exclude maintenance / system activity
    AND head_blocker_query NOT LIKE '%ALTER INDEX%'
    AND head_blocker_query NOT LIKE '%CREATE%INDEX%'
    AND head_blocker_query NOT LIKE '%DBCC%'
    AND head_blocker_query NOT LIKE '%BACKUP%'
    AND head_blocker_query NOT LIKE '%CHECKDB%'
    AND head_blocker_query NOT LIKE '%UPDATE STATISTICS%'
    AND head_blocker_query NOT LIKE '%sp_updatestats%'
    AND head_blocker_query NOT LIKE '%IndexOptimize%'
    AND head_blocker_query NOT LIKE '%Maintenance%'
)
INSERT INTO #HeadBlockerSummary
(
    head_blocker_session_id,
    head_blocker_query,
    blocking_queries_count,
    BlockingDuration_HHMMSS,
    BlockingDuration_Minutes,
    logdate,
    Status
)
SELECT
    head_blocker_session_id,
    head_blocker_query,
    blocking_queries_count,
    duration AS BlockingDuration_HHMMSS,
    BlockingDuration_Minutes,
    logdate,
    CASE
        WHEN BlockingDuration_Minutes >= 10 
             OR blocking_queries_count >= 5 THEN 'ACTION REQUIRED'
        WHEN BlockingDuration_Minutes BETWEEN 5 AND 9 
             OR blocking_queries_count BETWEEN 2 AND 4 THEN 'WATCH'
        ELSE 'OK'
    END AS Status
FROM DedupHeadBlockers
WHERE rn = 1;



/*********************/  
/****** HTML Preparation *********/  
/*********************/  
  
DECLARE @TableHTML  VARCHAR(MAX),                                    
  @StrSubject VARCHAR(100),                                 
  @Version VARCHAR(250),                                
  @Edition VARCHAR(100),                                
  @SP VARCHAR(100),          
  @URL varchar(1000),                                
  @Str varchar(1000),                                
  @NoofCriErrors varchar(3)       
           
  
SELECT @Version = @@version                                
SELECT @Edition = CONVERT(VARCHAR(50), serverproperty('Edition'))                                                               
SELECT @SP = CONVERT(VARCHAR(50), SERVERPROPERTY ('productlevel'))                                                                 
SELECT @strSubject = ISNULL(@ClientName, 'ClientName') + ' Database Server Health Check Report (' + ISNULL(@Server, @@SERVERNAME)+ ')';
                                    


SET @TableHTML = 
'<font face="Verdana" size="2">
My Dear DBA,<br><br>
Good Morning,<br><br>
Please review the SQL Server health check information below.  
This report provides key observations and analysis collected during the health check window.  
Kindly review and take necessary action where required.<br><br>
</font>';


 SET                                   
 @TableHTML =     @TableHTML +                                
 '</table>                                  
 <font face="Verdana" size="4"><b>SQL Server Details  </b></font>  
 <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                
 <tr>                                
 <td align="Center" width="50%" bgColor="001F3D" height="15"><b>                                
 <font face="Verdana" color="#ffffff" size="1">Version</font></b></td>                                
 <td align="Center" width="17%" bgColor="001F3D" height="15"><b>                                
 <font face="Verdana" color="#ffffff" size="1">Edition</font></b></td>                                
 <td align="Center" width="35%" bgColor="001F3D" height="15"><b>                                
 <font face="Verdana" color="#ffffff" size="1">Service Pack</font></b></td>                                                                
 </tr>                                
 <tr>                                
 <td align="Center" width="50%" height="27"><font face="Verdana" size="1">'+@version +'</font></td>                                
 <td align="Center" width="17%" height="27"><font face="Verdana" size="1">'+@edition+'</font></td>                                
 <td align="Center" width="18%" height="27"><font face="Verdana" size="1">'+@SP+'</font></td>                                
 </tr> '                               
 
 --AG Report

--  IF EXISTS (SELECT 1 FROM #AG_DB_Status)
--BEGIN					
--SELECT                                   
-- @TableHTML = @TableHTML +                                   
-- '</table>                          
-- <p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>                                  
-- <font face="Verdana" size="4">High Availability Database Status </font><br><br>' +
   
-- '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="1500" bgColor="#ffffff" borderColorLight="#000000" border="1"> 
 
-- <tr>                                    
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">AG_Name</font></th>   
-- <th align="Center" width="300" bgColor="001F3D">                                   
-- <font face="Verdana" size="1" color="#FFFFFF">DatabaseName</font></th>                                    
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">PrimaryServer</font></th>                                   
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SecondaryServer</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">AvailabilityMode</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">FailoverMode</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SynchronizationState</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SynchronizationHealth</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SuspendReason</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LogSendQueue_KB</font></th>
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LogSendRate_KB_per_sec</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">RedoQueue_KB</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">RedoRate_KB_per_sec</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SecondaryLag_Seconds</font></th> 
-- </tr>'                                    
--END

--SELECT                                   
-- @TableHTML = @TableHTML +                                      
--'<tr><td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), AG_Name),'') + '</font></td>' +                       
-- '<td align="Center"><font face="Verdana" size="1" >' + ISNULL(CONVERT(VARCHAR(50), DatabaseName),'') + '</font></td>' +                                  
-- '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), PrimaryServer),'') +'</font></td>' + 
--  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SecondaryServer),'') +'</font></td>' +    
--   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), AvailabilityMode),'') +'</font></td>' +    
--    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), FailoverMode),'') +'</font></td>' +    
--	 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SynchronizationState),'') +'</font></td>' +    
--	  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SynchronizationHealth),'') +'</font></td>' +    
--	   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SuspendReason),'') +'</font></td>' +    
--	    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LogSendQueue_KB),'') +'</font></td>' +    
--		 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LogSendRate_KB_per_sec),'') +'</font></td>' +    
--		  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), RedoQueue_KB),'') +'</font></td>' +    
--		   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), RedoRate_KB_per_sec),'') +'</font></td>' +    
--		   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SecondaryLag_Seconds),'') +'</font></td>'  
--FROM #AG_DB_Status

 /**Log Shipping Status****/
-- IF EXISTS (SELECT 1 FROM #LogShippingStatus)
--BEGIN

--SELECT                                   
-- @TableHTML = @TableHTML +                                   
-- '</table>                          
-- <p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>                                  
-- <font face="Verdana" size="4">Log Shipping Database Details</font><br><br>' +
   
-- '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="1500" bgColor="#ffffff" borderColorLight="#000000" border="1"> 
 
-- <tr>                                    
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">PrimaryServer</font></th>   
-- <th align="Center" width="300" bgColor="001F3D">                                   
-- <font face="Verdana" size="1" color="#FFFFFF">PrimaryDatabase</font></th>                                    
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">BackupDirectory</font></th>                                   
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">BackupShare</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LastBackupFile</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LastBackupDate</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">BackupCompression</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SecondaryServer</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SecondaryDatabase</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LastCopiedFile</font></th>
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LastCopiedDate</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LastRestoredFile</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LastRestoredDate</font></th> 
--  <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">LastRestoredLatency</font></th> 
--   <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">HealthStatus</font></th> 
-- </tr>'                                    
--END

--SELECT                                   
-- @TableHTML = @TableHTML +                                      
--'<tr><td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), PrimaryServer),'') + '</font></td>' +                       
-- '<td align="Center"><font face="Verdana" size="1" >' + ISNULL(CONVERT(VARCHAR(50), PrimaryDatabase),'') + '</font></td>' +                                  
-- '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(MAX), BackupDirectory),'') +'</font></td>' + 
--  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(MAX), BackupShare),'') +'</font></td>' +    
--   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(MAX), LastBackupFile),'') +'</font></td>' +    
--    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LastBackupDate),'') +'</font></td>' +    
--	 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), BackupCompression),'') +'</font></td>' +    
--	  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SecondaryServer),'') +'</font></td>' +    
--	   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SecondaryDatabase),'') +'</font></td>' +    
--	    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(MAX), LastCopiedFile),'') +'</font></td>' +    
--		 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LastCopiedDate),'') +'</font></td>' +    
--		  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(MAX), LastRestoredFile),'') +'</font></td>' +    
--		   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LastRestoredDate),'') +'</font></td>' +    
--		   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LastRestoredLatency),'') +'</font></td>' +  
--		   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), HealthStatus),'') +'</font></td>' 
--FROM #LogShippingStatus


--IF EXISTS (SELECT 1 FROM #DBMirroringStatus)
--BEGIN

--SELECT                                   
-- @TableHTML = @TableHTML +                                   
-- '</table>                          
-- <p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>                                  
-- <font face="Verdana" size="4">Mirroring Database Details</font><br><br>' +
   
-- '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="1500" bgColor="#ffffff" borderColorLight="#000000" border="1"> 
 
-- <tr>                                    
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">DatabaseName</font></th>   
-- <th align="Center" width="300" bgColor="001F3D">                                   
-- <font face="Verdana" size="1" color="#FFFFFF">Role</font></th>                                    
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">State</font></th>                                   
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">MirrorServer</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">MirrorServerInstance</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">                                  
-- <font face="Verdana" size="1" color="#FFFFFF">SafetyLevel</font></th> 
-- </tr>'                                    
--END

--SELECT                                   
-- @TableHTML = @TableHTML +                                      
--'<tr><td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), DatabaseName),'') + '</font></td>' +                       
-- '<td align="Center"><font face="Verdana" size="1" >' + ISNULL(CONVERT(VARCHAR(50), Role),'') + '</font></td>' +                                  
-- '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), State),'') +'</font></td>' + 
--  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), MirrorServer),'') +'</font></td>' +    
--   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), MirrorServerInstance),'') +'</font></td>' +    
--    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), SafetyLevel),'') +'</font></td>' 
--FROM #DBMirroringStatus

  SELECT                                   
 @TableHTML = @TableHTML +                                     
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>Services Last Recycled Status  </b></font>  
 <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="933" bgColor="#ffffff" borderColorLight="#000000" border="1">                                      
 <tr>                                      
 <th align="Center" width="50" bgColor="001F3D">                                      
  <font face="Verdana" size="1" color="#FFFFFF">Service_Name</font></th> 
  <th align="Center" width="50" bgColor="001F3D">                                      
  <font face="Verdana" size="1" color="#FFFFFF">Service_Status</font></th> 
  <th align="Center" width="50" bgColor="001F3D">                                      
  <font face="Verdana" size="1" color="#FFFFFF">Last Recycle Date & Time</font></th> 
 <th align="Center" width="50" bgColor="001F3D">                                      
  <font face="Verdana" size="1" color="#FFFFFF">Current Date & Time</font></th>                                      
 <th align="Center" width="50" bgColor="001F3D">                                   
 <font face="Verdana" size="1" color="#FFFFFF">UpTimeInDays</font></th>                                      
  </tr>'  
   
SELECT                                   
 @TableHTML =  @TableHTML +                                       
 '<tr>                                    
 <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100), ServiceName ), '')  +'</font></td>' +                                        
'<td align="Center"><font face="Verdana" size="1" color="' +
        CASE WHEN ServiceStatus = 'Running' THEN '#40C211' ELSE '#FF0000' END + '">' + 
        ISNULL(CONVERT(VARCHAR(10), ServiceStatus), '') +
    '</font></td>' +                                   
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  RestartTime ), '')  +'</font></td>' +  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  CurrentTime ), '')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  UpTimeInDays ), '')  +'</font></td>' +
  '</tr>'                                  
FROM                                   
 #RebootDetails 

 /** CPU Usage ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>CPU Usage Last 24 Hours </b></font>        
 <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Minimum_Utilization (%)</font></th>               
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Maximum_Utilization (%)</font></th>               
 <th align="Center" width="250" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Average_Utilization (%)</font></th>               
 <th align="Center" width="250" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Maximum_Utilization Count</font></th>              
   </tr>'                                  
SELECT                                   
 @TableHTML =  @TableHTML +                                     
 '<tr>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), Min_Total_CPU ), '')  +'</font></td>' +    
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), Max_Total_CPU ), '')  +'</font></td>' +                              
   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), Avg_Total_CPU ), '')  +'</font></td>'+   
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), Max_CPU_Hit_Count ), '')  +'</font></td></tr>'
FROM                                   
 #CPU  

  /** PLE Status ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>PLE Status Last 24 Hours </b></font>        
 <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Minimum_PLE (Sec)</font></th>               
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Maximum_PLE (Sec)</font></th>               
 <th align="Center" width="250" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Average_PLE (Sec)</font></th>               
 <th align="Center" width="250" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">PLE < 300 (Sec) Count</font></th>              
   </tr>'                                  
SELECT                                   
 @TableHTML =  @TableHTML +                                     
 '<tr>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), MinPLE ), '')  +'</font></td>' +    
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), MaxPLE ), '')  +'</font></td>' +                              
   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), AvgPLE ), '')  +'</font></td>'+   
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), PLE_Less_Than_300_Count ), '')  +'</font></td></tr>'
FROM                                   
 #PLE
                                                     
/** Free Disk Space Report ***/  
 
SELECT
@TableHTML = @TableHTML +
'</table>

<p style="margin-top:0; margin-bottom:0">&nbsp;</p>

<font face="Verdana" size="4"><b>Disk Space Usage</b></font>

<!-- Small space after heading -->
<table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr>
        <td height="6">&nbsp;</td>
    </tr>
</table>

<table style="BORDER-COLLAPSE: collapse"
       borderColor="#111111"
       cellPadding="0"
       width="933"
       bgColor="#ffffff"
       borderColorLight="#000000"
       border="1">
<tr>
    <th align="Center" width="150" bgColor="#001F3D">
        <font face="Verdana" size="1" color="#FFFFFF">Drive Letter</font>
    </th>
    <th align="Center" width="150" bgColor="#001F3D">
        <font face="Verdana" size="1" color="#FFFFFF">TotalSpace_GB</font>
    </th>
    <th align="Center" width="150" bgColor="#001F3D">
        <font face="Verdana" size="1" color="#FFFFFF">UsedSpace_GB</font>
    </th>
    <th align="Center" width="150" bgColor="#001F3D">
        <font face="Verdana" size="1" color="#FFFFFF">FreeSpace_GB</font>
    </th>
    <th align="Center" width="150" bgColor="#001F3D">
        <font face="Verdana" size="1" color="#FFFFFF">Percentage_Full</font>
    </th>
</tr>';
                                 
                                  
SELECT                                   
 @TableHTML = @TableHTML +   
'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), Drive_Letter),'') + '</font></td>' +  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), TotalSpace_GB),'') + '</font></td>' +                                  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), UsedSpace_GB),'') +'</font></td>' +     
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(max), FreeSpace_GB),'') +'</font></td>' +
CASE WHEN Percentage_Full > 90 THEN
  '<td align="Center"><font face="Verdana" size="1" color="#FF0000"><b>' + ISNULL(CONVERT(VARCHAR(100),  Percentage_Full), '')  +'</font></td>'
ELSE  
  '<td align="Center"><font face="Verdana" size="1" color="#40C211"><b>' + ISNULL(CONVERT(VARCHAR(100),  Percentage_Full), '')  +'</font></td>'
  END +
  '</tr>'
FROM #driveinfo order By Drive_Letter ASC
 
  
/** Tempdb File Usage ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>Tempdb File Usage</b></font>  
  <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Database Name</font></th>               
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">File Name</font></th>               
 <th align="Center" width="250" bgColor="001F3D"> 
 <font face="Verdana" size="1" color="#FFFFFF">Physical Name</font></th>               
 <th align="Center" width="250" bgColor="001F3D">                                
 <font face="Verdana" size="1" color="#FFFFFF">FileSize_GB</font></th>               
 <th align="Center" width="200" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">Available_GB</font></th>               
 <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Percentage_Full </font></th>  
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Total_Disk_Space_GB </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Available_Disk_Space_GB </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Auto_Growth_Status </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Auto_Growth_Value </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Auto_Growth_Max_Limit </font></th> 
   </tr>'                                  
SELECT
    @TableHTML = @TableHTML +
    '<tr>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(databasename, '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(filename, '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(physicalName, '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), filesizeGB), '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), availableSpaceGB), '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1" color="' +
        CASE WHEN percentfull > 80 THEN '#FF0000' ELSE '#40C211' END + '">' +
        ISNULL(CONVERT(VARCHAR(10), percentfull), '') +
    '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), diskTotalGB), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), diskFreeGB), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), AutoGrowth), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), AutoGrowthSetting), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), MaxSize), '') + '</font></td>' +
    '</tr>'
FROM #tempdbfileusage;
     
  
 
 /** UserDB File Usage ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>User Database File Usage</b></font> 
  <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
<table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>

 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Database Name</font></th>               
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">File Name</font></th> 
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">File Type</font></th>
 <th align="Center" width="250" bgColor="001F3D"> 
 <font face="Verdana" size="1" color="#FFFFFF">Physical Name</font></th>               
 <th align="Center" width="250" bgColor="001F3D">                                
 <font face="Verdana" size="1" color="#FFFFFF">FileSize_GB</font></th>               
 <th align="Center" width="200" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">Available_GB</font></th>               
 <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Percentage_Full </font></th>  
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Total_Disk_Space_GB </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Available_Disk_Space_GB </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Auto_Growth_Status </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Auto_Growth_Value </font></th> 
  <th align="Center" width="200" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Auto_Growth_Max_Limit </font></th>                
   </tr>'                                  
SELECT
    @TableHTML = @TableHTML +
    '<tr>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(databasename, '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(filename, '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(filetype, '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(physicalName, '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), filesizeGB), '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), availableSpaceGB), '') + '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1" color="' +
        CASE WHEN percentfull > 80 THEN '#FF0000' ELSE '#40C211' END + '">' +
        ISNULL(CONVERT(VARCHAR(10), percentfull), '') +
    '</font></td>' +
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), diskTotalGB), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), diskFreeGB), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), AutoGrowth), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), AutoGrowthSetting), '') + '</font></td>' +
	'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), MaxSize), '') + '</font></td>' +
    '</tr>'
FROM #UserDBFileUsage;
   
/** Database Backup Report ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>Backup Report for User Database</b></font>
   <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Database_Name</font></th>                              
  <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">LastFullBackupTime</font></th> 
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">FullBackupSize_GB</font></th>
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">LastDiffBackupTime</font></th> 
 <th align="Center" width="300" bgColor="001F3D">              
 <font face="Verdana" size="1" color="#FFFFFF">DiffBackupSize_GB</font></th>
 <th align="Center" width="300" bgColor="001F3D">              
 <font face="Verdana" size="1" color="#FFFFFF">LastLogBackupTime</font></th> 
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">LogBackupSize_MB</font></th>
   </tr>'                                  
SELECT      
 @TableHTML = @TableHTML +                                       
 '<tr>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(DatabaseName, '') + '</font></td>' +
 CASE WHEN LastFullBackupTime IS NULL OR DATEDIFF(DAY, LastFullBackupTime, GETDATE()) > 7 THEN '<td align="Center"><font face="Verdana" size="1" color="#FF0000"><b>' + ISNULL(CONVERT(VARCHAR(19), LastFullBackupTime, 120), '') + '</b></font></td>'
 ELSE '<td align="Center"><font face="Verdana" size="1" color="#40C211">'+ CONVERT(VARCHAR(19), LastFullBackupTime, 120) + '</font></td>' END +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), FullBackupSize_GB), '') + '</font></td>' +
 CASE WHEN LastDiffBackupTime IS NULL OR DATEDIFF(DAY, LastDiffBackupTime, GETDATE()) > 2 THEN '<td align="Center"><font face="Verdana" size="1" color="#FF0000"><b>' + ISNULL(CONVERT(VARCHAR(19), LastDiffBackupTime, 120), '') + '</b></font></td>'
 ELSE '<td align="Center"><font face="Verdana" size="1" color="#40C211">' + CONVERT(VARCHAR(19), LastDiffBackupTime, 120) + '</font></td>' END +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), DiffBackupSize_GB), '') + '</font></td>' +
 CASE WHEN LastLogBackupTime IS NULL OR DATEDIFF(HOUR, LastLogBackupTime, GETDATE()) > 2  THEN '<td align="Center"><font face="Verdana" size="1" color="#FF0000"><b>' + ISNULL(CONVERT(VARCHAR(19), LastLogBackupTime, 120), '') + '</b></font></td>'
 ELSE '<td align="Center"><font face="Verdana" size="1" color="#40C211">' + CONVERT(VARCHAR(19), LastLogBackupTime, 120) + '</font></td>' END +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(20), LogBackupSize_MB), '') + '</font></td>' + '</tr>'
FROM #Backup_Report 
 
   
 /**Failed Jobs in Last 24Hrs****/
 IF EXISTS (SELECT 1 FROM #JobFailureSummary)
BEGIN

SELECT                                   
 @TableHTML = @TableHTML +                                   
 '</table>                          
 <p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>Failed Jobs in Last 24Hrs</b></font><br><br>' +
   
 '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="1500" bgColor="#ffffff" borderColorLight="#000000" border="1"> 
 
 <tr>                                    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Job Name</font></th>   
 <th align="Center" width="300" bgColor="001F3D">                                   
 <font face="Verdana" size="1" color="#FFFFFF">Failure_Count</font></th>                                    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">First_Failure_Time</font></th>                                   
 <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Last_Failure_Time</font></th>   
  <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Last_Failed_Step</font></th> 
  <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Job_Owner</font></th> 
  <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Job_Category</font></th> 
  <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Job_Status</font></th> 
 </tr>'                                    
END

SELECT                                   
 @TableHTML = @TableHTML +                                      
'<tr><td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), JobName),'') + '</font></td>' +                       
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(5), FailureCount_Last24Hrs),'') + '</font></td>' +                                  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), FirstFailureTime_UTC),'') +'</font></td>' +  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LastFailureTime_UTC),'') +'</font></td>' + 
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), LastFailedStep),'') +'</font></td>' + 
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), JobOwner),'') +'</font></td>' + 
'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), JobCategory),'') +'</font></td>' + 
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(10), JobStatus),'') +'</font></td></tr>'      
FROM #JobFailureSummary


-- Initialize HTML table variable
SELECT @TableHTML = @TableHTML +
 '</table>                          
 <p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4"><b>Wait Statistics Report in Last 24Hrs</b></font><br><br>' +
   
 '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="1500" bgColor="#ffffff" borderColorLight="#000000" border="1"> 
 
 <tr>                                    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Date & Time</font></th>   
 <th align="Center" width="300" bgColor="001F3D">                                   
 <font face="Verdana" size="1" color="#FFFFFF">CPU (%)</font></th>                                    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">IO Data (%)</font></th>                                   
 <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">IO Log (%)</font></th>   
  <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Memory (%)</font></th> 
  <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Lock (%)</font></th> 
   <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Network (%)</font></th> 
  <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Over all Status</font></th> 
 </tr>'  

-- Build table rows with color-coded flags
SELECT @TableHTML = @TableHTML +
'<tr>' +
'<td align="Center"><font face="Verdana" size="1">' + CONVERT(VARCHAR, DateHour, 120) + '</font></td>' +

-- CPU column with color
'<td align="Center"><font face="Verdana" size="1" color="' +
    CASE 
        WHEN CPU_Pct > 40 THEN '#FF0000'       -- red
        WHEN CPU_Pct BETWEEN 20 AND 40 THEN '#FFD700' -- yellow
        ELSE '#40C211'                         -- green
    END + '">' +
    CONVERT(VARCHAR(10), CPU_Pct) +
'</font></td>' +

-- IO_Data column
'<td align="Center"><font face="Verdana" size="1" color="' +
    CASE 
        WHEN IO_Data_Pct > 40 THEN '#FF0000'
        WHEN IO_Data_Pct BETWEEN 25 AND 40 THEN '#FFD700'
        ELSE '#40C211'
    END + '">' +
    CONVERT(VARCHAR(10), IO_Data_Pct) +
'</font></td>' +

-- IO_Log column
'<td align="Center"><font face="Verdana" size="1" color="' +
    CASE 
        WHEN IO_Log_Pct >= 20 THEN '#FF0000'
        ELSE '#40C211'
    END + '">' +
    CONVERT(VARCHAR(10), IO_Log_Pct) +
'</font></td>' +

-- Memory column
'<td align="Center"><font face="Verdana" size="1" color="' +
    CASE 
        WHEN Memory_Pct > 0 THEN '#FFD700'
        ELSE '#40C211'
    END + '">' +
    CONVERT(VARCHAR(10), Memory_Pct) +
'</font></td>' +

-- Lock column
'<td align="Center"><font face="Verdana" size="1" color="' +
    CASE 
        WHEN Lock_Pct >= 10 THEN '#FF0000'
        ELSE '#40C211'
    END + '">' +
    CONVERT(VARCHAR(10), Lock_Pct) +
'</font></td>' +

-- ✅ Network IO
'<td align="Center"><font face="Verdana" size="1" color="' +
    CASE 
        WHEN Network_Pct > 15 THEN '#FF0000'
        WHEN Network_Pct BETWEEN 5 AND 15 THEN '#FFD700'
        ELSE '#40C211'
    END + '">' +
    CONVERT(VARCHAR(10), Network_Pct) +
'</font></td>' +

-- Overall Status
'<td align="Center" style="font-weight:bold;"><font face="Verdana" size="1" color="' +
CASE 
    WHEN CPU_Pct > 40
      OR IO_Data_Pct > 40
      OR IO_Log_Pct >= 20
      OR Lock_Pct >= 10
      OR Network_Pct > 15
        THEN '#FF0000'

    WHEN CPU_Pct BETWEEN 20 AND 40
      OR IO_Data_Pct BETWEEN 25 AND 40
      OR Memory_Pct > 0
      OR Network_Pct BETWEEN 5 AND 15
        THEN '#FFCC00'

    ELSE '#40C211'
END + '">' +

CASE 
    WHEN CPU_Pct > 40
      OR IO_Data_Pct > 40
      OR IO_Log_Pct >= 20
      OR Lock_Pct >= 10
      OR Network_Pct > 15
        THEN 'ACTION'

    WHEN CPU_Pct BETWEEN 20 AND 40
      OR IO_Data_Pct BETWEEN 25 AND 40
      OR Memory_Pct > 0
      OR Network_Pct BETWEEN 5 AND 15
        THEN 'WATCH'

    ELSE 'OK'
END +

'</font></td>' +
'</tr>'
FROM #WaitStatsHourly;

SELECT @TableHTML = @TableHTML + '</table>';



/******** Long Running Transactions ********/

-- ===== Always show heading =====
SELECT @TableHTML = @TableHTML +
'<p style="margin-top:5; margin-bottom:5">&nbsp;</p>
<font face="Verdana" size="4">
<b>Long Running Queries Summary in Last 24 Hours</b>
</font>';

IF EXISTS (SELECT 1 FROM #LongRunningSummary)
BEGIN
    /* ===== Table Header ===== */
    SELECT @TableHTML = @TableHTML +
    '<table width="933" cellpadding="0" cellspacing="0" border="0">
        <tr><td height="6">&nbsp;</td></tr>
     </table>

     <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse"
            borderColor="#111111" cellPadding="0"
            width="933" border="1">
        <tr>
            <th align="Center" bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">SummaryDate</font></th>
            <th align="Center" bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">&lt;5 Min</font></th>
            <th align="Center" bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">5-10 Min</font></th>
            <th align="Center" bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">10-25 Min</font></th>
            <th align="Center" bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">&gt;25 Min</font></th>
            <th align="Center" bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Total Queries</font></th>
        </tr>';

    /* ===== Table Rows ===== */
    SELECT @TableHTML = @TableHTML +
    '<tr>
        <td align="Center"><font face="Verdana" size="1">' + CONVERT(VARCHAR(19), SummaryDate, 120) + '</font></td>
        <td align="Center"><font face="Verdana" size="1">' + CONVERT(VARCHAR(10), Less_5_Min) + '</font></td>
        <td align="Center"><font face="Verdana" size="1">' + CONVERT(VARCHAR(10), Between_5_10_Min) + '</font></td>
        <td align="Center"><font face="Verdana" size="1">' + CONVERT(VARCHAR(10), Between_10_25_Min) + '</font></td>
        <td align="Center"><font face="Verdana" size="1">' + CONVERT(VARCHAR(10), Greater_25_Min) + '</font></td>
        <td align="Center"><font face="Verdana" size="1">' + CONVERT(VARCHAR(10), TotalQueries) + '</font></td>
     </tr>'
    FROM #LongRunningSummary;

    -- Close table
    SELECT @TableHTML = @TableHTML + '</table>';
END
ELSE
BEGIN
    /* ===== No data message ===== */
    SELECT @TableHTML = @TableHTML +
    '<p style="margin-top:10px; margin-bottom:10px">
        <font face="Verdana" size="3" color="#008000">
            No Long Running queries found in the last 24 hours
        </font>
     </p>';
END



 /** Head Blocking Information **/   						
/**************** Blocking Information ****************/
SELECT @TableHTML = @TableHTML +
'<p style="margin-top: 5; margin-bottom: 5">&nbsp;</p>
<font face="Verdana" size="4"><b>Blocked Queries Report in Last 24 Hours</b></font>';

IF EXISTS (SELECT 1 FROM #HeadBlockerSummary)
BEGIN
    /* ===== Table Header ===== */
    SELECT @TableHTML = @TableHTML +
    '<table width="933" cellpadding="0" cellspacing="0" border="0">
        <tr><td height="6">&nbsp;</td></tr>
     </table>

     <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse"
            borderColor="#111111" cellPadding="3"
            width="933" border="1">
        <tr>
            <th bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Head Blocker SPID</font></th>
            <th bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Head Blocker Query</font></th>
            <th bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Blocked Count</font></th>
            <th bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Duration (HH:MM:SS)</font></th>
            <th bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Duration (Minutes)</font></th>
            <th bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Log Date</font></th>
            <th bgColor="001F3D"><font face="Verdana" size="1" color="#FFFFFF">Status</font></th>
        </tr>';

    /* ===== Table Rows ===== */
    SELECT
        @TableHTML = @TableHTML +
        '<tr>
            <td align="center"><font face="Verdana" size="1">' +
                ISNULL(CONVERT(VARCHAR(10), head_blocker_session_id), '') + '</font></td>

            <td><font face="Verdana" size="1">' +
                ISNULL(head_blocker_query, '') + '</font></td>

            <td align="center"><font face="Verdana" size="1">' +
                ISNULL(CONVERT(VARCHAR(10), blocking_queries_count), '') + '</font></td>

            <td align="center"><font face="Verdana" size="1">' +
                ISNULL(BlockingDuration_HHMMSS, '') + '</font></td>

            <td align="center"><font face="Verdana" size="1">' +
                ISNULL(CONVERT(VARCHAR(10), BlockingDuration_Minutes), '') + '</font></td>

            <td align="center"><font face="Verdana" size="1">' +
                ISNULL(CONVERT(VARCHAR(19), logdate, 120), '') + '</font></td>

            <td align="center" style="font-weight:bold;">
                <font face="Verdana" size="1" color="' +

                CASE Status
                    WHEN 'ACTION REQUIRED' THEN '#FF0000'
                    WHEN 'WATCH'         THEN '#FFCC00'
                    WHEN 'OK'             THEN  '#008000'
                    ELSE '#000000'
                END +

                '">' + Status + '</font>
            </td>
        </tr>'
    FROM #HeadBlockerSummary
    ORDER BY logdate DESC;
-- CLOSE THE TABLE HERE
SELECT @TableHTML = @TableHTML + '</table>';

END
ELSE
BEGIN
    /* ===== No data message ===== */
    SELECT @TableHTML = @TableHTML +
    '<p style="margin-top:10px; margin-bottom:10px">
        <font face="Verdana" size="3" color="#008000">
            No Head Blocker queries found in the last 24 hours
        </font>
     </p>';
END
-- CLOSE THE TABLE HERE
SELECT @TableHTML = @TableHTML + '</table>';



		
 
 /** Error Log Report Last 24 Hours***/  
-- SELECT                                   
-- @TableHTML =  @TableHTML +                              
-- '</table>                                  
-- <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
-- <font face="Verdana" size="4">Error Log Report in Last 24 Hours</font>
--   <table width="933" cellpadding="0" cellspacing="0" border="0">
--    <tr><td height="6">&nbsp;</td></tr>
--</table>
-- <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
--   <tr>    
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">ErrorMessage</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">                                    
-- <font face="Verdana" size="1" color="#FFFFFF">ErrorCount</font></th>                              
--  <th align="Center" width="300" bgColor="001F3D">               
-- <font face="Verdana" size="1" color="#FFFFFF">FirstOccurrence</font></th> 
-- <th align="Center" width="300" bgColor="001F3D">               
-- <font face="Verdana" size="1" color="#FFFFFF">LastOccurrence</font></th>
--   </tr>'                                  
--SELECT      
-- @TableHTML =  @TableHTML +                                       
-- '<tr>    
-- <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(MAX),  ErrorMessage), '')  +'</font></td>' +  
--  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  ErrorCount), 'NULL')  +'</font></td>' + 
-- '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(varchar, FirstOccurrence, 120), '')  +'</font></td>' +  
-- '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(varchar, LastOccurrence, 120), '')  +'</font></td>' + 
   
--  '</tr>'                                  
--FROM             
-- #ErrorLogSummary order by ErrorCount desc

SET @TableHTML = @TableHTML +
'<br><br>
<font face="Verdana" size="2">
Thanks &amp; Regards,<br>
SQL Server<br>
' + @SERVERNAME + ' 
</font>';



    
EXEC msdb.dbo.sp_send_dbmail                                    
 @profile_name = @MailProfile,                       
 @recipients=@MailID,                                   
 @subject = @strSubject,                                   
 @body = @TableHTML,                                      
 @body_format = 'HTML' ;                               
  
  
DROP TABLE  #RebootDetails;    
DROP TABLE  #CPU; 
DROP TABLE #PLE;
DROP TABLE  #Backup_Report;   
DROP TABLE  #tempdbfileusage; 
DROP TABLE  #UserDBFileUsage;
DROP TABLE  #JobFailureSummary;
Drop TABLE #LongRunningSummary;
Drop TABLE #HeadBlockerSummary
Drop TABLE #driveinfo;
Drop TABLE #output;
DROP TABLE #WaitStatsHourly;
  
SET NOCOUNT OFF;  
SET ARITHABORT OFF;  
END  
 

GO


DECLARE @RC int
DECLARE @MailProfile nvarchar(200)
DECLARE @MailID nvarchar(2000)
DECLARE @Server varchar(100)
 
-- TODO: Set parameter values here.
 
EXECUTE @RC = [DBADB].[dbo].[SQLhealthcheck_report_new2]
 'DBA'
,'aruneswaran@geopits.com;'--nareshkumar.s@geopits.com --mssqltechsupport@geopits.com
,'EC2AMAZ-IC6PG05'
,'Retail Scan'
GO