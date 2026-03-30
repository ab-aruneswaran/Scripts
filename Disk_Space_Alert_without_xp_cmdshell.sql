USE [msdb]
GO



/****** Object:  Job [DBA_Disk_Space_Alert]    Script Date: 13-11-2025 22:56:03 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 13-11-2025 22:56:03 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA_Disk_Space_Alert', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT -- login change
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Disk_Space_Alert]    Script Date: 13-11-2025 22:56:04 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Disk_Space_Alert', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'IF OBJECT_ID(''tempdb..##temp'') IS NOT NULL 
    DROP TABLE ##temp;

DECLARE @profile VARCHAR(100) = ''SQL_Training''; --Change  Database Mail profile
DECLARE @recipient VARCHAR(MAX) = ''aruneswaran@geopits.com''; --Change Recipient
DECLARE @cc VARCHAR(MAX) = '''';
DECLARE @bcc VARCHAR(MAX) = '''';
DECLARE @body NVARCHAR(MAX);
DECLARE @sub VARCHAR(200) = ''Local Disk Space Alert: '' + @@SERVERNAME + '' ('' + CONVERT(VARCHAR, GETDATE(), 107) + '')''; --Client Name
DECLARE @svrName VARCHAR(255) = @@SERVERNAME;

CREATE TABLE ##temp (
    DriveLetter VARCHAR(10),
    TotalSpace_GB DECIMAL(10,2),
    UsedSpace_GB DECIMAL(10,2),
    FreeSpace_GB DECIMAL(10,2),
    Percentage_Free DECIMAL(5,2)
);

-- Collect drive details
;WITH VolumeInfo AS
(
    SELECT DISTINCT
        vs.volume_mount_point AS DriveLetter,
        vs.total_bytes / 1024.0 / 1024.0 / 1024.0 AS TotalSpace_GB,
        (vs.total_bytes - vs.available_bytes) / 1024.0 / 1024.0 / 1024.0 AS UsedSpace_GB,
        vs.available_bytes / 1024.0 / 1024.0 / 1024.0 AS FreeSpace_GB,
        CAST(100.0 * vs.available_bytes / vs.total_bytes AS DECIMAL(5,2)) AS Percentage_Free
    FROM sys.master_files AS mf
    CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
)
INSERT INTO ##temp
SELECT DriveLetter, TotalSpace_GB, UsedSpace_GB, FreeSpace_GB, Percentage_Free
FROM VolumeInfo
ORDER BY DriveLetter;

-- Build HTML email body
DECLARE @xml NVARCHAR(MAX);
SELECT @xml = CAST((
    SELECT 
        td = DriveLetter, '''',
        td = CAST(TotalSpace_GB AS VARCHAR(20)), '''',
        td = CAST(UsedSpace_GB AS VARCHAR(20)), '''',
        td = CAST(FreeSpace_GB AS VARCHAR(20)), '''',
        td = CAST(Percentage_Free AS VARCHAR(10))
    FROM ##temp
    WHERE Percentage_Free < 10.00
    FOR XML PATH(''tr''), ELEMENTS
) AS NVARCHAR(MAX));

SET @body = 
N''<html>
    <head>
        <style>
            table, th, td {
                border: 1px solid black;
                border-collapse: collapse;
                text-align: center;
                padding: 5px;
            }
            th { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <h2>Low Disk Space Alert: '' + @svrName + ''</h2>
        <table>
            <tr>
                <th>Drive Name</th>
                <th>Total Size (GB)</th>
                <th>Used Space (GB)</th>
                <th>Free Space (GB)</th>
                <th>Free %</th>
            </tr>'' + ISNULL(@xml, N'''') + ''
        </table>
    </body>
</html>'';

-- Send alert if low disk space found
IF (@xml IS NOT NULL)
BEGIN
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = @profile,
        @body = @body,
        @body_format = ''HTML'',
        @recipients = @recipient,
        @copy_recipients = @cc,
        @blind_copy_recipients = @bcc,
        @subject = @sub;
END

DROP TABLE ##temp;
', 
		@database_name=N'DBADB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every_6_Hours', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=6, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20251113, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'69e49655-7205-4fa7-aa04-0a711f6c0374'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


