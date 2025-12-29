USE [DBADB]
GO
/****** Object:  StoredProcedure [dbo].[SQLhealthcheck_report_new1]    Script Date: 12/23/2025 11:12:54 AM ******/
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
--   Last 4 days Critical Errors from ErrorLog  
--   Instance Last Recycle Information  
--   Tempdb File Usage  
--             Free Disk Space Report
--   CPU Usage  
--   Memory Usage  
--   Performance Counters Data  
--   Missing Backup Report  
--   Connection Information  
--   Log Space Usage Report  
--   Job Status Report  
--   Blocking Report  
--   Long running Transactions
--   Failed Jobs in Last 24Hrs
/**************************/  
/**************************/  
CREATE   PROCEDURE [dbo].[SQLhealthcheck_report_new1] (  
  @MailProfile NVARCHAR(200),   
  @MailID NVARCHAR(2000),  
  @Server VARCHAR(max) = NULL)  
AS  
BEGIN  
SET NOCOUNT ON;  
SET ARITHABORT ON;  
  
DECLARE @ServerName VARCHAR(max);  
SET @ServerName = ISNULL(@Server,@@SERVERNAME);  
  
/*********************/  
/****** Server Reboot Details ********/  
/*********************/  
  
CREATE TABLE #RebootDetails                                
(                                
 LastRecycle datetime,                                
 CurrentDate datetime,                                
 UpTimeInDays varchar(100)                          
)                        
Insert into #RebootDetails          
SELECT sqlserver_start_time 'Last Recycle',GetDate() 'Current Date', DATEDIFF(DD, sqlserver_start_time,GETDATE())'Up Time in Days'  
FROM sys.dm_os_sys_info;  
  
/*********************/  
/****** Errors audit for last 4 Days *****/  
/*********************/  
 

IF OBJECT_ID('tempdb..#ErrorLogInfo') IS NOT NULL
    DROP TABLE #ErrorLogInfo;

CREATE TABLE #ErrorLogInfo
(
    LogDate     DATETIME,
    ProcessInfo VARCHAR(50),
    LogText     VARCHAR(4000)
);

DECLARE @StartDate DATETIME = DATEADD(HOUR, -24, GETDATE());
DECLARE @EndDate   DATETIME = GETDATE();

INSERT INTO #ErrorLogInfo
EXEC xp_readerrorlog
    0,          -- Current error log
    1,          -- SQL Server error log
    NULL,       -- No filter
    NULL,       -- No filter
    @StartDate,
    @EndDate,
    'DESC';

CREATE NONCLUSTERED INDEX IX_ErrorLogInfo
ON #ErrorLogInfo (LogDate)
INCLUDE (LogText);

IF OBJECT_ID('tempdb..#ErrorLogSummary') IS NOT NULL
    DROP TABLE #ErrorLogSummary;

CREATE TABLE #ErrorLogSummary
(
    ErrorMessage     VARCHAR(500),
    ErrorCount       INT,
    FirstOccurrence  DATETIME,
    LastOccurrence   DATETIME
);
INSERT INTO #ErrorLogSummary
(
    ErrorMessage,
    ErrorCount,
    FirstOccurrence,
    LastOccurrence
)
SELECT
    LEFT(LogText, 500) AS ErrorMessage,
    COUNT(*) AS ErrorCount,
    MIN(LogDate) AS FirstOccurrence,
    MAX(LogDate) AS LastOccurrence
FROM #ErrorLogInfo
GROUP BY LEFT(LogText, 500);



  
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

-- Optional: Print header and preview result

  
/*********************/  
/***** SQL Server CPU Usage Details ******/  
/*********************/  
/*********************/
/***** SQL Server CPU Usage Summary (Last 24 Hours) ******/
/*********************/
CREATE TABLE #CPU
(
    servername      VARCHAR(50),
    Max_Total_CPU   VARCHAR(50),
    Min_Total_CPU   VARCHAR(50),
    Avg_Total_CPU   VARCHAR(50),
    load_date       VARCHAR(50)
);
     
  
INSERT INTO #CPU
(
    servername,
    Max_Total_CPU,
    Min_Total_CPU,
    Avg_Total_CPU,
    load_date
)
SELECT
    @@SERVERNAME,
    MAX(SQLUtilisedCPU + Otherprosses),
    MIN(SQLUtilisedCPU + Otherprosses),
    AVG(SQLUtilisedCPU + Otherprosses),
    GETDATE()
FROM DBADB.dbo.CPUUtilisationdata
WHERE Time >= DATEADD(HOUR, -24, GETDATE());

/*********************/  
/***** SQL Server Memory Usage Details *****/  
/*********************/  
  
CREATE TABLE #Memory_BPool (  
BPool_Committed_MB VARCHAR(50),  
BPool_Commit_Tgt_MB VARCHAR(50),  
BPool_Visible_MB VARCHAR(50));  
  
-- SQL server 2008 / 2008 R2  
/**  
-- SQL server 2012 / 2014 / 2016  
INSERT INTO #Memory_BPool   
SELECT  
      (committed_kb)/1024.0 as BPool_Committed_MB,  
      (committed_target_kb)/1024.0 as BPool_Commit_Tgt_MB,  
      (visible_target_kb)/1024.0 as BPool_Visible_MB  
FROM  sys.dm_os_sys_info;  
**/  
CREATE TABLE #Memory_sys (  
total_physical_memory_mb VARCHAR(50),  
available_physical_memory_mb VARCHAR(50),  
total_page_file_mb VARCHAR(50),  
available_page_file_mb VARCHAR(50),  
Percentage_Used VARCHAR(50),  
system_memory_state_desc VARCHAR(50));  
  
INSERT INTO #Memory_sys  
select  
      total_physical_memory_kb/1024 AS total_physical_memory_mb,  
      available_physical_memory_kb/1024 AS available_physical_memory_mb,  
      total_page_file_kb/1024 AS total_page_file_mb,  
      available_page_file_kb/1024 AS available_page_file_mb,  
      100 - (100 * CAST(available_physical_memory_kb AS DECIMAL(18,3))/CAST(total_physical_memory_kb AS DECIMAL(18,3)))   
      AS 'Percentage_Used',  
      system_memory_state_desc  
from  sys.dm_os_sys_memory;  
  
  
CREATE TABLE #Memory_process(  
physical_memory_in_use_GB VARCHAR(50),  
locked_page_allocations_GB VARCHAR(50),  
virtual_address_space_committed_GB VARCHAR(50),  
available_commit_limit_GB VARCHAR(50),  
page_fault_count VARCHAR(50))  
  
INSERT INTO #Memory_process  
select  
      physical_memory_in_use_kb/1048576.0 AS 'physical_memory_in_use(GB)',  
      locked_page_allocations_kb/1048576.0 AS 'locked_page_allocations(GB)',  
      virtual_address_space_committed_kb/1048576.0 AS 'virtual_address_space_committed(GB)',  
      available_commit_limit_kb/1048576.0 AS 'available_commit_limit(GB)',  
      page_fault_count as 'page_fault_count'  
from  sys.dm_os_process_memory;  
  
  
CREATE TABLE #Memory(  
Parameter VARCHAR(200),  
Value VARCHAR(100));  
  
INSERT INTO #Memory   
SELECT 'BPool_Committed_MB',BPool_Committed_MB FROM #Memory_BPool  
UNION  
SELECT 'BPool_Commit_Tgt_MB', BPool_Commit_Tgt_MB FROM #Memory_BPool  
UNION   
SELECT 'BPool_Visible_MB', BPool_Visible_MB FROM #Memory_BPool  
UNION  
SELECT 'total_physical_memory_mb',total_physical_memory_mb FROM #Memory_sys  
UNION  
SELECT 'available_physical_memory_mb',available_physical_memory_mb FROM #Memory_sys  
UNION  
SELECT 'total_page_file_mb',total_page_file_mb FROM #Memory_sys  
UNION  
SELECT 'available_page_file_mb',available_page_file_mb FROM #Memory_sys  
UNION  
SELECT 'Percentage_Used',Percentage_Used FROM #Memory_sys  
UNION  
SELECT 'system_memory_state_desc',system_memory_state_desc FROM #Memory_sys  
UNION  
SELECT 'physical_memory_in_use_GB',physical_memory_in_use_GB FROM #Memory_process  
UNION  
SELECT 'locked_page_allocations_GB',locked_page_allocations_GB FROM #Memory_process  
UNION  
SELECT 'virtual_address_space_committed_GB',virtual_address_space_committed_GB FROM #Memory_process  
UNION  
SELECT 'available_commit_limit_GB',available_commit_limit_GB FROM #Memory_process  
UNION  
SELECT 'page_fault_count',page_fault_count FROM #Memory_process;  

  
/**********************/  
/***** Performance Counter Details ********/  
/**********************/  
  
CREATE TABLE #PerfCntr_Data(  
Parameter VARCHAR(300),  
Value VARCHAR(100));  
  
-- Get size of SQL Server Page in bytes  
DECLARE @pg_size INT, @Instancename varchar(max)  
SELECT @pg_size = low from master..spt_values where number = 1 and type = 'E'  
  
-- Extract perfmon counters to a temporary table  
IF OBJECT_ID('tempdb..#perfmon_counters') is not null DROP TABLE #perfmon_counters  
SELECT * INTO #perfmon_counters FROM sys.dm_os_performance_counters;  
  
-- Get SQL Server instance name as it require for capturing Buffer Cache hit Ratio  
SELECT  @Instancename = LEFT([object_name], (CHARINDEX(':',[object_name])))   
FROM    #perfmon_counters   
WHERE   counter_name = 'Buffer cache hit ratio';  
  
INSERT INTO #PerfCntr_Data  
SELECT CONVERT(VARCHAR(300),Cntr) AS Parameter, CONVERT(VARCHAR(100),Value) AS Value  
FROM  
(  
SELECT  'Total Server Memory (GB)' as Cntr,  
        (cntr_value/1048576.0) AS Value   
FROM    #perfmon_counters   
WHERE   counter_name = 'Total Server Memory (KB)'  
UNION ALL  
SELECT  'Target Server Memory (GB)',   
        (cntr_value/1048576.0)   
FROM    #perfmon_counters   
WHERE   counter_name = 'Target Server Memory (KB)'  
UNION ALL  
SELECT  'Connection Memory (MB)',   
        (cntr_value/1024.0)   
FROM    #perfmon_counters   
WHERE   counter_name = 'Connection Memory (KB)'  
UNION ALL  
SELECT  'Lock Memory (MB)',   
        (cntr_value/1024.0)   
FROM    #perfmon_counters   
WHERE   counter_name = 'Lock Memory (KB)'  
UNION ALL  
SELECT  'SQL Cache Memory (MB)',   
        (cntr_value/1024.0)   
FROM    #perfmon_counters   
WHERE   counter_name = 'SQL Cache Memory (KB)'  
UNION ALL  
SELECT  'Optimizer Memory (MB)',   
        (cntr_value/1024.0)   
FROM    #perfmon_counters   
WHERE   counter_name = 'Optimizer Memory (KB) '  
UNION ALL  
SELECT  'Granted Workspace Memory (MB)',   
        (cntr_value/1024.0)   
FROM    #perfmon_counters   
WHERE   counter_name = 'Granted Workspace Memory (KB) '  
UNION ALL  
SELECT  'Cursor memory usage (MB)',   
        (cntr_value/1024.0)   
FROM    #perfmon_counters   
WHERE   counter_name = 'Cursor memory usage' and instance_name = '_Total'  
UNION ALL  
SELECT  'Total pages Size (MB)',   
        (cntr_value*@pg_size)/1048576.0   
FROM    #perfmon_counters   
WHERE   object_name= @Instancename+'Buffer Manager'   
        and counter_name = 'Total pages'  
UNION ALL  
SELECT  'Database pages (MB)',   
        (cntr_value*@pg_size)/1048576.0   
FROM    #perfmon_counters   
WHERE   object_name = @Instancename+'Buffer Manager' and counter_name = 'Database pages'  
UNION ALL  
SELECT  'Free pages (MB)',   
        (cntr_value*@pg_size)/1048576.0   
FROM    #perfmon_counters   
WHERE   object_name = @Instancename+'Buffer Manager'   
        and counter_name = 'Free pages'  
UNION ALL  
SELECT  'Reserved pages (MB)',   
        (cntr_value*@pg_size)/1048576.0   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Buffer Manager'   
        and counter_name = 'Reserved pages'  
UNION ALL  
SELECT  'Stolen pages (MB)',   
        (cntr_value*@pg_size)/1048576.0   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Buffer Manager'   
        and counter_name = 'Stolen pages'  
UNION ALL  
SELECT  'Cache Pages (MB)',   
        (cntr_value*@pg_size)/1048576.0   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Plan Cache'   
        and counter_name = 'Cache Pages' and instance_name = '_Total'  
UNION ALL  
SELECT  'Page Life Expectency in seconds',  
        cntr_value   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Buffer Manager'   
        and counter_name = 'Page life expectancy'  
UNION ALL  
SELECT  'Free list stalls/sec',  
        cntr_value   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Buffer Manager'   
        and counter_name = 'Free list stalls/sec'  
UNION ALL  
SELECT  'Checkpoint pages/sec',  
        cntr_value   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Buffer Manager'   
        and counter_name = 'Checkpoint pages/sec'  
UNION ALL  
SELECT  'Lazy writes/sec',  
        cntr_value   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Buffer Manager'   
        and counter_name = 'Lazy writes/sec'  
UNION ALL  
SELECT  'Memory Grants Pending',  
        cntr_value   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Memory Manager'   
        and counter_name = 'Memory Grants Pending'  
UNION ALL  
SELECT  'Memory Grants Outstanding',  
        cntr_value   
FROM    #perfmon_counters   
WHERE   object_name=@Instancename+'Memory Manager'   
        and counter_name = 'Memory Grants Outstanding'  
UNION ALL  
SELECT  'process_physical_memory_low',  
        process_physical_memory_low   
FROM    sys.dm_os_process_memory WITH (NOLOCK)  
UNION ALL  
SELECT  'process_virtual_memory_low',  
        process_virtual_memory_low   
FROM    sys.dm_os_process_memory WITH (NOLOCK)  
UNION ALL  
SELECT  'Max_Server_Memory (MB)' ,  
        [value_in_use]   
FROM    sys.configurations   
WHERE   [name] = 'max server memory (MB)'  
UNION ALL  
SELECT  'Min_Server_Memory (MB)' ,  
        [value_in_use]   
FROM    sys.configurations   
WHERE   [name] = 'min server memory (MB)'  
UNION ALL  
SELECT  'BufferCacheHitRatio',  
        (a.cntr_value * 1.0 / b.cntr_value) * 100.0   
FROM    sys.dm_os_performance_counters a  
        JOIN (SELECT cntr_value,OBJECT_NAME FROM sys.dm_os_performance_counters  
              WHERE counter_name = 'Buffer cache hit ratio base' AND   
                    OBJECT_NAME = @Instancename+'Buffer Manager') b ON   
                    a.OBJECT_NAME = b.OBJECT_NAME WHERE a.counter_name = 'Buffer cache hit ratio'   
                    AND a.OBJECT_NAME = @Instancename+'Buffer Manager') AS P;  
  
  
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
    LogBackupSize_GB       DECIMAL(18,2)
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
    LogBackupSize_GB
)
SELECT
    d.name AS DatabaseName,

    fb.backup_finish_date AS LastFullBackupTime,
    fb.BackupSize_GB      AS FullBackupSize_GB,

    db.backup_finish_date AS LastDiffBackupTime,
    db.BackupSize_GB      AS DiffBackupSize_GB,

    lb.backup_finish_date AS LastLogBackupTime,
    lb.BackupSize_GB      AS LogBackupSize_GB

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

WHERE d.database_id > 4
ORDER BY d.name;

-- View result
--SELECT * FROM #Backup_Report;

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
/****** Failed Jobs in Last 24Hrs ********/  
/*********************/
 
create table #Failed_jobs(date_time varchar(100),
job_name varchar(200),
job_step varchar(10),
error_message varchar(max))
 
insert into #Failed_jobs
SELECT MSDB.dbo.agent_datetime(jh.run_date,jh.run_time) as date_time
,j.name as job_name,js.step_id as job_step,jh.message as error_message
FROM msdb.dbo.sysjobs AS j
INNER JOIN msdb.dbo.sysjobsteps AS js ON js.job_id = j.job_id
INNER JOIN msdb.dbo.sysjobhistory AS jh ON jh.job_id = j.job_id AND jh.step_id = js.step_id
WHERE jh.run_status = 0 AND MSDB.dbo.agent_datetime(jh.run_date,jh.run_time) >= GETDATE()-1 and j.[name] <> 'geomon_test'
ORDER BY MSDB.dbo.agent_datetime(jh.run_date,jh.run_time) DESC
  

/*********************/  
/***** Currently Running Jobs Info *******/  
/*********************/  
Create table #JobInfo(               
spid varchar(10),                           
lastwaittype varchar(100),                           
dbname varchar(100),                           
login_time varchar(100),                           
status varchar(100),                           
opentran varchar(100),                           
hostname varchar(100),                          
JobName varchar(100),                          
command nvarchar(2000),  
domain varchar(100),   
loginname varchar(100)     
)   
insert into #JobInfo  
SELECT  distinct p.spid,p.lastwaittype,DB_NAME(p.dbid),p.login_time,p.status,p.open_tran,p.hostname,J.name,  
p.cmd,p.nt_domain,p.loginame  
FROM master..sysprocesses p  
INNER JOIN msdb..sysjobs j ON   
substring(left(j.job_id,8),7,2) + substring(left(j.job_id,8),5,2) + substring(left(j.job_id,8),3,2) + substring(left(j.job_id,8),1,2) = substring(p.program_name, 32, 8)   
Inner join msdb..sysjobactivity sj on j.job_id=sj.job_id  
WHERE program_name like'SQLAgent - TSQL JobStep (Job %' and sj.stop_execution_date is null  

/*********************/  
/****** Tempdb File Info *********/  
/*********************/  
-- tempdb file usage  
Create table #tempdbfileusage(               
servername varchar(max),                           
databasename varchar(max),                           
filename varchar(max),
physicalName varchar(max),                           
filesizeGB varchar(max),                           
availableSpaceGB varchar(max),                           
percentfull varchar(max)   
)   
  
DECLARE @TEMPDBSQL NVARCHAR(4000);

SET @TEMPDBSQL = '
USE tempdb;

SELECT
    CONVERT(VARCHAR(MAX), @@SERVERNAME) AS server_name,
    db.name AS database_name,
    mf.name AS file_logical_name,
    mf.filename AS file_physical_name,

    -- Total File Size in GB
    CAST(mf.size / 128.0 / 1024 AS DECIMAL(18,2)) AS file_size_gb,

    -- Available Space in GB
    CAST(
        (mf.size / 128.0 - CAST(FILEPROPERTY(mf.name, ''SpaceUsed'') AS INT) / 128.0)
        / 1024
        AS DECIMAL(18,2)
    ) AS available_space_gb,

    -- Percent Full
    CAST(
        (CAST(FILEPROPERTY(mf.name, ''SpaceUsed'') AS FLOAT) / mf.size) * 100
        AS DECIMAL(5,2)
    ) AS percent_full

FROM tempdb.dbo.sysfiles mf
JOIN master..sysdatabases db
    ON db.dbid = DB_ID();
';

-- Insert data
INSERT INTO #tempdbfileusage
EXEC sp_executesql @TEMPDBSQL;

/*********************/  
/****** User DB File Info *********/  
/*********************/  
-- usr db file usage  

CREATE TABLE #UserDBFileUsage
(
    servername        VARCHAR(128),
    databasename      VARCHAR(MAX), 
    filename          VARCHAR(MAX),
    Type              VARCHAR(MAX),
    physicalName      VARCHAR(MAX),
    filesizeGB        VARCHAR(MAX),
    availableSpaceGB  VARCHAR(MAX),
    percentfull       VARCHAR(MAX)
);

DECLARE @SQL1 NVARCHAR(MAX) = N'';

-- Iterate through each database to build the dynamic SQL
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
    SET @SQL1 = @SQL1 + '
    USE ' + QUOTENAME(@dbname) + ';
    INSERT INTO #UserDBFileUsage
    SELECT
        @@SERVERNAME AS servername,
        DB_NAME() AS databasename,
        mf.name AS filename,
        CASE mf.type WHEN 0 THEN ''DATA'' WHEN 1 THEN ''LOG'' END AS FileType,
        mf.physical_name AS physicalName,
        CAST(mf.size / 128.0 / 1024 AS DECIMAL(18,2)) AS filesizeGB,
        CAST((mf.size / 128.0 - CAST(FILEPROPERTY(mf.name, ''SpaceUsed'') AS INT) / 128.0) / 1024 AS DECIMAL(18,2)) AS availableSpaceGB,
        CAST((CAST(FILEPROPERTY(mf.name, ''SpaceUsed'') AS FLOAT) / mf.size) * 100 AS DECIMAL(5,2)) AS percentfull
    FROM sys.database_files mf;
    ';

    FETCH NEXT FROM db_cursor INTO @dbname;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

-- Execute the dynamic SQL
EXEC sys.sp_executesql @SQL1;

/*********************/  
/****** Long Running Info *********/  
/*********************/  
IF OBJECT_ID('tempdb..#LongRunningQueries') IS NOT NULL
    DROP TABLE #LongRunningQueries;-- Create a temporary table
CREATE TABLE #LongRunningQueries
(
    StartTime DATETIME,
    ElapsedTime NVARCHAR(128),
    SPID INT,
    UserName NVARCHAR(128),
    ProgramName NVARCHAR(128),
    DatabaseName NVARCHAR(128),
    ExecutingSQL NVARCHAR(MAX),
    WaitType NVARCHAR(60),
    StatementText NVARCHAR(MAX),
    StoredProcedure NVARCHAR(128)
);

-- Insert the query results into the temp table
WITH CTE AS
(
    SELECT *,
           ROW_NUMBER() OVER
           (
               PARTITION BY SPID, DatabaseName, ISNULL(StatementText, ExecutingSQL)
               ORDER BY logdate DESC
           ) AS rn
    FROM [DBADB].[dbo].[longqrydetails]
    WHERE logdate >= DATEADD(HOUR, -24, GETDATE())
)
, MaxElapsed AS
(
    SELECT 
        SPID,
        DatabaseName,
        ISNULL(StatementText, ExecutingSQL) AS QueryText,
        MAX(ElapsedTime) AS MaxElapsedTime
    FROM CTE
    GROUP BY SPID, DatabaseName, ISNULL(StatementText, ExecutingSQL)
)
INSERT INTO #LongRunningQueries
SELECT 
    c.StartTime,
    m.MaxElapsedTime AS ElapsedTime,
    c.SPID,
    c.UserName,
    c.ProgramName,
    c.DatabaseName,
    c.ExecutingSQL,
    c.WaitType,
    c.StatementText,
    c.StoredProcedure
FROM CTE c
INNER JOIN MaxElapsed m
    ON c.SPID = m.SPID
    AND c.DatabaseName = m.DatabaseName
    AND ISNULL(c.StatementText, c.ExecutingSQL) = m.QueryText
WHERE c.rn = 1 and c.DatabaseName  NOT IN ('DBADB')
ORDER BY m.MaxElapsedTime DESC;

/*********************/  
/****** Blocking Queries Info *********/  
/*********************/  
IF OBJECT_ID('tempdb..#BlockedQueriesLast24Hrs') IS NOT NULL
    DROP TABLE #BlockedQueriesLast24Hrs;-- Create a temporary table
CREATE TABLE #BlockedQueriesLast24Hrs
(
    BlockedSessionID INT,
    StartTime DATETIME,
    WaitingInMinutes INT,
    WaitType NVARCHAR(120),
    BlockingSessionID INT,
    QueryWaiting NVARCHAR(MAX)
);

;WITH CTE AS
(
    SELECT BlockedSessionID,
        BlockingSessionID,
        StartTime,
        WaitingInMinutes,
        WaitType,
        QueryWaiting,
           ROW_NUMBER() OVER
           (
               PARTITION BY 
                   BlockedSessionID,
                   BlockingSessionID,
                   ISNULL(QueryWaiting, '')
               ORDER BY logdate DESC
           ) AS rn
    FROM [DBADB].[dbo].[BlockedQueriesInfo]
    WHERE StartTime >= DATEADD(HOUR, -24, GETDATE())
)
INSERT INTO #BlockedQueriesLast24Hrs
SELECT
    BlockedSessionID,
    StartTime,
    WaitingInMinutes,
    WaitType,
    BlockingSessionID,
    QueryWaiting
FROM CTE
WHERE rn = 1;




/*********************/  
/****** Wait Stats *********/  
/*********************/  
IF OBJECT_ID('tempdb..##WaitStatsLast24Hrss') IS NOT NULL
    DROP TABLE #WaitStatsLast24Hrs;-- Create a temporary table

	-- Create temporary table
CREATE TABLE #WaitStatsLast24Hrs
(
    WaitType NVARCHAR(60),
    Total_Wait_S FLOAT,
    Total_Signal_S FLOAT,
    Total_WaitCount INT,
    Avg_Wait_S FLOAT
);

-- Insert aggregated data into temp table
INSERT INTO #WaitStatsLast24Hrs
SELECT  
    WaitType,
    SUM(Wait_S)        AS Total_Wait_S,
    SUM(Signal_S)      AS Total_Signal_S,
    SUM(WaitCount)     AS Total_WaitCount,
    AVG(AverageWait_S) AS Avg_Wait_S
FROM [DBADB].[dbo].[WaitStatData]
WHERE [Date] >= DATEADD(HOUR, -24, GETDATE())
GROUP BY WaitType
ORDER BY SUM(Wait_S) DESC;

/*********************/  
/****** HTML Preparation *********/  
/*********************/  
  
DECLARE @TableHTML  VARCHAR(MAX),                                    
  @StrSubject VARCHAR(100),                                    
  @Oriserver VARCHAR(100),                                
  @Version VARCHAR(250),                                
  @Edition VARCHAR(100),                                
  @ISClustered VARCHAR(100),                                
  @SP VARCHAR(100),                                
  @ServerCollation VARCHAR(100),                                
  @SingleUser VARCHAR(5),                                
  @LicenseType VARCHAR(100),                                
  @Cnt int,           
  @URL varchar(1000),                                
  @Str varchar(1000),                                
  @NoofCriErrors varchar(3)       
  
-- Variable Assignment              
  
SELECT @Version = @@version                                
SELECT @Edition = CONVERT(VARCHAR(100), serverproperty('Edition'))                                
SET @Cnt = 0                                
IF serverproperty('IsClustered') = 0                                 
BEGIN                                
 SELECT @ISClustered = 'No'                                
END                                
ELSE        
BEGIN                                
 SELECT @ISClustered = 'YES'                                
END                                
SELECT @SP = CONVERT(VARCHAR(100), SERVERPROPERTY ('productlevel'))                                
SELECT @ServerCollation = CONVERT(VARCHAR(100), SERVERPROPERTY ('Collation'))                                 
SELECT @LicenseType = CONVERT(VARCHAR(100), SERVERPROPERTY ('LicenseType'))                                 
SELECT @SingleUser = CASE SERVERPROPERTY ('IsSingleUser')                                
      WHEN 1 THEN 'Yes'                                
      WHEN 0 THEN 'No'                                
      ELSE                                
      'null' END                                
SELECT @OriServer = CONVERT(VARCHAR(50), SERVERPROPERTY('servername'))                                  
SELECT @strSubject = 'Retail Scan Database Server Health Check Report for  ('+ CONVERT(VARCHAR(100), @SERVERNAME) + ')'                                    

  
SET @TableHTML =
'<font face="Verdana" size="4">Health Check Report</font>
<br><br>

<table border="1" cellpadding="0" cellspacing="0"
       style="border-collapse: collapse"
       bordercolor="#111111"
       width="933"
       height="60">

<tr>
    <td align="Center" bgcolor="#001F3D">
        <font face="Verdana" size="2" color="#FFFFFF"><b>SQL Server Name</b></font>
    </td>
    <td align="Center" bgcolor="#001F3D">
        <font face="Verdana" size="2" color="#FFFFFF"><b>Date</b></font>
    </td>
    <td align="Center" bgcolor="#001F3D">
        <font face="Verdana" size="2" color="#FFFFFF"><b>Time</b></font>
    </td>
</tr>

<tr>
    <td align="Center">
        <font face="Verdana" size="2">' + @@SERVERNAME + '</font>
    </td>
    <td align="Center">
        <font face="Verdana" size="2">' + CONVERT(VARCHAR(10), GETDATE(), 105) + '</font>
    </td>
    <td align="Center">
    <font face="Verdana" size="2">' +
    FORMAT(GETDATE(), 'hh:mm tt') +
    '</font>
</td>
</tr>'

 SELECT                                   
 @TableHTML = @TableHTML +                                     
 '</table> 

 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">SQL Server Details  </font>  
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
 <td align="Center" width="60%" bgColor="001F3D" height="15"><b>                                
 <font face="Verdana" color="#ffffff" size="1">Collation</font></b></td>                                
 <td align="Center" width="93%" bgColor="001F3D" height="15"><b>                                
 <font face="Verdana" color="#ffffff" size="1">LicenseType</font></b></td>                                
 <td align="Center" width="40%" bgColor="001F3D" height="15"><b>                                
<font face="Verdana" color="#ffffff" size="1">SingleUser</font></b></td>                                
 <td align="Center" width="93%" bgColor="001F3D" height="15"><b>                                
 <font face="Verdana" color="#ffffff" size="1">Clustered</font></b></td>                                
 </tr>                                
 <tr>                                
 <td align="Center" width="50%" height="27"><font face="Verdana" size="1">'+@version +'</font></td>                                
 <td align="Center" width="17%" height="27"><font face="Verdana" size="1">'+@edition+'</font></td>                                
 <td align="Center" width="18%" height="27"><font face="Verdana" size="1">'+@SP+'</font></td>                                
 <td align="Center" width="17%" height="27"><font face="Verdana" size="1">'+@ServerCollation+'</font></td>                                
 <td align="Center" width="25%" height="27"><font face="Verdana" size="1">'+@LicenseType+'</font></td>                                
 <td align="Center" width="25%" height="27"><font face="Verdana" size="1">'+@SingleUser+'</font></td>                                
 <td align="Center" width="93%" height="27"><font face="Verdana" size="1">'+@isclustered+'</font></td>                                
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
 <font face="Verdana" size="4">Instance Last Recycled Status  </font>  
 <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="933" bgColor="#ffffff" borderColorLight="#000000" border="1">                                      
 <tr>                                      
 <th align="Center" width="50" bgColor="001F3D">                                      
  <font face="Verdana" size="1" color="#FFFFFF">Last Recycle Date</font></th>                                      
 <th align="Center" width="50" bgColor="001F3D">                                      
  <font face="Verdana" size="1" color="#FFFFFF">Current DateTime</font></th>                                      
 <th align="Center" width="50" bgColor="001F3D">                                   
 <font face="Verdana" size="1" color="#FFFFFF">UpTimeInDays</font></th>                                      
  </tr>'  
   
SELECT                                   
 @TableHTML =  @TableHTML +                                       
 '<tr>                                    
 <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100), LastRecycle ), '')  +'</font></td>' +                                        
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  CurrentDate ), '')  +'</font></td>' +                                   
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  UpTimeInDays ), '')  +'</font></td>' +                                        
  '</tr>'                                  
FROM                                   
 #RebootDetails 

 /** CPU Usage ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">CPU Usage Last 24 Hours </font>        
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
              
   </tr>'                                  
SELECT                                   
 @TableHTML =  @TableHTML +                                     
 '<tr>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100), Min_Total_CPU ), '')  +'</font></td>' +    
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100), Max_Total_CPU ), '')  +'</font></td>' +                              
   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100), Avg_Total_CPU ), '')  +'</font></td></tr>'                                  
FROM                                   
 #CPU  

 /** Memory Usage ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Memory Usage </font>  
 <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="left" width="136" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Parameter</font></th>                              
  <th align="left" width="200" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">Value</font></th>              
   </tr>'                                  
SELECT                                   
 @TableHTML =  @TableHTML +                                       
 '<tr>                                    
 <td><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(200),  Parameter ), '')  +'</font></td>' +                                        
 '<td><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  Value ), '')  +'</font></td>' +                                     
  '</tr>'                                  
FROM                                   
 #Memory;  

 /** Performance Counter Values ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Performance Counter Data</font>    
 <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="left" width="136" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Performance_Counter</font></th>                              
  <th align="left" width="200" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">Value</font></th>              
   </tr>'                                  
SELECT                                   
 @TableHTML =  @TableHTML +                                       
 '<tr>                                    
 <td><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(200),  Parameter ), '')  +'</font></td>' +                                        
 '<td><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  Value ), '')  +'</font></td>' +                                     
  '</tr>'                                  
FROM                                   
 #PerfCntr_Data;   

                              
                            
/** Free Disk Space Report ***/  
 
SELECT
@TableHTML = @TableHTML +
'</table>

<p style="margin-top:0; margin-bottom:0">&nbsp;</p>

<font face="Verdana" size="4">Disk Space Usage</font>

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
--'<td><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(max), Percentage_Free),'') +'</font></td></tr>'
CASE WHEN Percentage_Full > 90 THEN
  '<td align="Center"><font face="Verdana" size="1" color="#FF0000"><b>' + ISNULL(CONVERT(VARCHAR(100),  Percentage_Full), '')  +'</font></td>'
ELSE  
  '<td align="Center"><font face="Verdana" size="1" color="#40C211"><b>' + ISNULL(CONVERT(VARCHAR(100),  Percentage_Full), '')  +'</font></td>'
  END +
  '</tr>'
FROM #driveinfo
 
  
/** Tempdb File Usage ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Tempdb File Usage</font>  
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
   </tr>'                                  
select                                   
@TableHTML =  @TableHTML +                                     
 '<tr>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(databasename, '') + '</font></td>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(FileName, '') +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(physicalName, '') +'</font></td>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(filesizeGB, '') +'</font></td>' +                                  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(availableSpaceGB, '') +'</font></td>' +  
 CASE WHEN CONVERT(DECIMAL(10,3),percentfull) >80.00 THEN    
'<td align="Center"><font face="Verdana" size="1" color="#FF0000"><b>' + ISNULL(percentfull, '') +'</b></font></td></tr>'                                               
 ELSE  
 '<td align="Center"><font face="Verdana" size="1" color="#40C211"><b>' + ISNULL(CONVERT(VARCHAR(100),  percentfull), '')  +'</font></td>' END                                
from                                   
 #tempdbfileusage       
  
 
 /** UserDB File Usage ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">User Database File Usage</font> 
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
   </tr>'                                  
select                                   
@TableHTML =  @TableHTML +                                     
 '<tr>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(databasename, '') + '</font></td>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(FileName, '') +'</font></td>' + 
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(Type, '') +'</font></td>' +                                     
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(physicalName, '') +'</font></td>' +                                      
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(filesizeGB, '') +'</font></td>' +                                  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(availableSpaceGB, '') +'</font></td>' +  
 CASE WHEN CONVERT(DECIMAL(10,3),percentfull) >80.00 THEN    
'<td align="Center"><font face="Verdana" size="1" color="#FF0000"><b>' + ISNULL(percentfull, '') +'</b></font></td></tr>'                                               
 ELSE  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(percentfull, '') +'</font></td></tr>' END 
                               
from                                   
 #UserDBFileUsage    order by filesizeGB desc
   
/** Database Backup Report ***/  
SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Backup Report for User Database</font>
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
 <font face="Verdana" size="1" color="#FFFFFF">LogBackupSize_GB</font></th>
   </tr>'                                  
SELECT      
 @TableHTML =  @TableHTML +                                       
 '<tr>                                    
 <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  DatabaseName ), '')  +'</font></td>' +                                        
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  LastFullBackupTime), 'NULL')  +'</font></td>' +    
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  FullBackupSize_GB), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  LastDiffBackupTime), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  DiffBackupSize_GB), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  LastLogBackupTime), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  LogBackupSize_GB), 'NULL')  +'</font></td>' +
  '</tr>'                                  
FROM             
 #Backup_Report  
  
 /** Connection Information ***/  
  
       
      

 
 /**Failed Jobs in Last 24Hrs****/
 IF EXISTS (SELECT 1 FROM #Failed_jobs)
BEGIN

SELECT                                   
 @TableHTML = @TableHTML +                                   
 '</table>                          
 <p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Failed Jobs in Last 24Hrs</font><br><br>' +
   
 '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="1500" bgColor="#ffffff" borderColorLight="#000000" border="1"> 
 
 <tr>                                    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Date & Time</font></th>   
 <th align="Center" width="300" bgColor="001F3D">                                   
 <font face="Verdana" size="1" color="#FFFFFF">Job_name</font></th>                                    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Failure_Job_Step_No</font></th>                                   
 <th align="Center" width="300" bgColor="001F3D">                                  
 <font face="Verdana" size="1" color="#FFFFFF">Error_Message</font></th>                                                                    
 </tr>'                                    
END

SELECT                                   
 @TableHTML = @TableHTML +                                      
'<tr><td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), date_time),'') + '</font></td>' +                       
 '<td align="Center"><font face="Verdana" size="1" color="#FF0000">' + ISNULL(CONVERT(VARCHAR(50), job_name),'') + '</font></td>' +                                  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), job_step),'') +'</font></td>' +     
  '<td><font face="Verdana" size="1" color="#FF0000">' + ISNULL(CONVERT(VARCHAR(max), error_message),'') +'</font></td></tr>'      
FROM #Failed_jobs
  
  
/*** Job Info ****/  
 IF EXISTS (SELECT 1 FROM #JobInfo)
BEGIN
SELECT                                   
 @TableHTML = @TableHTML +                                   
 '</table>                          
 <p style="margin-top: 0; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Job Status</font><br><br>' +                                      
 '<table style="BORDER-COLLAPSE: collapse" borderColor="#111111" cellPadding="0" width="933" bgColor="#ffffff" borderColorLight="#000000" border="1">                                    
 <tr>                                    
 <th align="Center" width="430" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Session_ID</font></th>   
 <th align="Center" width="430" bgColor="001F3D">                                     
 <font face="Verdana" size="1" color="#FFFFFF">Wait_Type</font></th>                                    
 <th align="Center" width="430" bgColor="001F3D">                                     
 <font face="Verdana" size="1" color="#FFFFFF">DatabaseName</font></th>                                    
 <th align="Center" width="430" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Process Loged Time</font></th>                                    
 <th align="Center" width="430" bgColor="001F3D">                                     
 <font face="Verdana" size="1" color="#FFFFFF">Status</font></th>                                    
 <th align="Center" width="430" bgColor="001F3D">                                     
 <font face="Verdana" size="1" color="#FFFFFF">Transaction_Status</font></th>      
  <th align="Center" width="430" bgColor="001F3D">                                     
 <font face="Verdana" size="1" color="#FFFFFF">HostName</font></th>    
  <th align="left" width="146" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">JobName</font></th>    
  <th align="Center" width="430" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">Command</font></th>    
  <th align="Center" width="430" bgColor="001F3D">                                     
 <font face="Verdana" size="1" color="#FFFFFF">Domain</font></th>     
   <th align="Center" width="430" bgColor="001F3D">                                     
 <font face="Verdana" size="1" color="#FFFFFF">LoginName</font></th>                                 
 </tr>'                                    
END

SELECT                                   
 @TableHTML = ISNULL(CONVERT(VARCHAR(MAX), @TableHTML), 'No Job Running') + '<tr><td align="Center><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100), spid), '') +'</font></td>' +                                      
'<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), lastwaittype),'') + '</font></td>' +                       
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), dbname),'') + '</font></td>' +                                  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), login_time),'') +'</font></td>' +     
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), status),'') +'</font></td>' +     
   '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), opentran),'') +'</font></td>' +     
    '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), hostname),'') +'</font></td>' +     
     '<td align="Center"> <font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(500), JobName),'') +'</font></td>' +     
      '<td><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(200), command),'') +'</font></td>' +     
        '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50), domain),'') +'</font></td>' +     
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(50),loginname ),'') + '</font></td></tr>'      
FROM                                   
 #JobInfo  

 /** Wait Stats***/  
 SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Wait Statistics </font>
   <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">WaitType</font></th>                              
  <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">Total_Wait_S</font></th> 
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">Total_Signal_S</font></th>
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">Total_WaitCount</font></th> 
 <th align="Center" width="300" bgColor="001F3D">              
 <font face="Verdana" size="1" color="#FFFFFF">Avg_Wait_S</font></th>
   </tr>'                                  
SELECT      
 @TableHTML =  @TableHTML +                                       
 '<tr>                                    
 <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  WaitType ), '')  +'</font></td>' +                                        
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  Total_Wait_S), 'NULL')  +'</font></td>' +    
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  Total_Signal_S), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  Total_WaitCount), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  Avg_Wait_S), 'NULL')  +'</font></td>' +
  '</tr>'                                  
FROM             
 #WaitStatsLast24Hrs  order by Total_Wait_S desc
  
  
/** Long running Transactions***/  
 SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Long Running Queries Report in Last 24 Hours</font>
   <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">SPID</font></th> 
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">StartTime</font></th>                              
  <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">ElapsedTime</font></th> 
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">UserName</font></th>
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">ProgramName</font></th> 
 <th align="Center" width="300" bgColor="001F3D">              
 <font face="Verdana" size="1" color="#FFFFFF">DatabaseName</font></th>
 <th align="Center" width="300" bgColor="001F3D">              
 <font face="Verdana" size="1" color="#FFFFFF">ExecutingSQL</font></th> 
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">WaitType</font></th>
  <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">StatementText</font></th>
  <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">StoredProcedure</font></th>
   </tr>'                                  
SELECT      
 @TableHTML =  @TableHTML +                                       
 '<tr>    
 <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  SPID), '')  +'</font></td>' +                                        
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(varchar, StartTime, 120), '')  +'</font></td>' +                                        
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  ElapsedTime), 'NULL')  +'</font></td>' +    
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  UserName), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  ProgramName), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  DatabaseName), 'NULL')  +'</font></td>' +
 '<td align="Left"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(max),  ExecutingSQL), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  WaitType), 'NULL')  +'</font></td>' +
 '<td align="Left"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(Max),  StatementText), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  StoredProcedure), 'NULL')  +'</font></td>' +
  '</tr>'                                  
FROM             
 #LongRunningQueries order by ElapsedTime desc

  /** Blocking Information **/  

  						
  SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Blocking Queries Report in Last 24 Hours</font>
   <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>                
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">BlockedSessionID</font></th>                              
  <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">StartTime</font></th> 
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">WaitingInMinutes</font></th>
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">WaitType</font></th> 
 <th align="Center" width="300" bgColor="001F3D">              
 <font face="Verdana" size="1" color="#FFFFFF">BlockingSessionID</font></th>
 <th align="Center" width="300" bgColor="001F3D">              
 <font face="Verdana" size="1" color="#FFFFFF">QueryWaiting</font></th>
   </tr>'                                  
SELECT      
 @TableHTML =  @TableHTML +                                       
 '<tr>                                    
 <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  BlockedSessionID ), '')  +'</font></td>' +                                        
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  StartTime), 'NULL')  +'</font></td>' +    
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  WaitingInMinutes), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  WaitType), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(max),  BlockingSessionID), 'NULL')  +'</font></td>' +
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  QueryWaiting), 'NULL')  +'</font></td>' +
  '</tr>'                                  
FROM             
 #BlockedQueriesLast24Hrs order by WaitingInMinutes desc				
 
 /** Error Log Report Last 24 Hours***/  
 SELECT                                   
 @TableHTML =  @TableHTML +                              
 '</table>                                  
 <p style="margin-top: 1; margin-bottom: 0">&nbsp;</p>                                  
 <font face="Verdana" size="4">Error Log Report in Last 24 Hours</font>
   <table width="933" cellpadding="0" cellspacing="0" border="0">
    <tr><td height="6">&nbsp;</td></tr>
</table>
 <table id="AutoNumber1" style="BORDER-COLLAPSE: collapse" borderColor="#111111" height="40" cellSpacing="0" cellPadding="0" width="933" border="1">                                  
   <tr>    
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">ErrorMessage</font></th> 
 <th align="Center" width="300" bgColor="001F3D">                                    
 <font face="Verdana" size="1" color="#FFFFFF">ErrorCount</font></th>                              
  <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">FirstOccurrence</font></th> 
 <th align="Center" width="300" bgColor="001F3D">               
 <font face="Verdana" size="1" color="#FFFFFF">LastOccurrence</font></th>
   </tr>'                                  
SELECT      
 @TableHTML =  @TableHTML +                                       
 '<tr>    
 <td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(MAX),  ErrorMessage), '')  +'</font></td>' +  
  '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(VARCHAR(100),  ErrorCount), 'NULL')  +'</font></td>' + 
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(varchar, FirstOccurrence, 120), '')  +'</font></td>' +  
 '<td align="Center"><font face="Verdana" size="1">' + ISNULL(CONVERT(varchar, LastOccurrence, 120), '')  +'</font></td>' + 
   
  '</tr>'                                  
FROM             
 #ErrorLogSummary order by ErrorCount desc



    
EXEC msdb.dbo.sp_send_dbmail                                    
 @profile_name = @MailProfile,                       
 @recipients=@MailID,                                   
 @subject = @strSubject,                                   
 @body = @TableHTML,                                      
 @body_format = 'HTML' ;                               
  
  
DROP TABLE  #RebootDetails  
--DROP TABLE  #ErrorLogInfo  
DROP TABLE  #CPU  
DROP TABLE  #Memory_BPool;  
DROP TABLE  #Memory_sys;  
DROP TABLE  #Memory_process;  
DROP TABLE  #Memory;  
DROP TABLE  #perfmon_counters;  
DROP TABLE  #PerfCntr_Data;  
DROP TABLE  #Backup_Report;  
DROP TABLE  #JobInfo;  
DROP TABLE  #tempdbfileusage; 
DROP TABLE  #UserDBFileUsage;
DROP TABLE  #Failed_jobs;
Drop TABLE #LongRunningQueries;
  
SET NOCOUNT OFF;  
SET ARITHABORT OFF;  
END  
 

DECLARE @RC int
DECLARE @MailProfile nvarchar(200)
DECLARE @MailID nvarchar(2000)
DECLARE @Server varchar(100)
 
-- TODO: Set parameter values here.
 
EXECUTE @RC = [DBADB].[dbo].[SQLhealthcheck_report_new1]
 'DBA'
,'mssqltechsupport@geopits.com'
,'EC2AMAZ-IC6PG05'
GO