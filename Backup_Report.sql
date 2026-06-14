CREATE OR ALTER PROCEDURE dbo.usp_Backup_Status_Report
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Body NVARCHAR(MAX)='',
            @Subject VARCHAR(200);

    SET @Subject = @@SERVERNAME + ' : Internal Backup Status Report';

    -------------------------------------------------------------------
    -- Refresh RDS Task Status Table
    -------------------------------------------------------------------
    TRUNCATE TABLE dbo.taskstatusresults_new;

    INSERT INTO dbo.taskstatusresults_new
    EXEC msdb.dbo.rds_task_status;

    -------------------------------------------------------------------
    -- Build HTML
    -------------------------------------------------------------------
    SET @Body =
    '<html>
    <body>
    <h3>RDS Backup Task Status</h3>

    <table border=1 cellspacing=0 cellpadding=4>
    <tr bgcolor="#D9EAD3">
        <th>Session ID</th>
        <th>Start Time</th>
        <th>Status</th>
        <th>Command</th>
        <th>% Complete</th>
        <th>Database Name</th>
        <th>ETA Completion Time</th>
        <th>Elapsed Min</th>
        <th>ETA Min</th>
        <th>ETA Hours</th>
        <th>Running Statement</th>
    </tr>';

    SELECT
        @Body = @Body +
        '<tr>
        <td>' + CAST(r.session_id AS VARCHAR(10)) + '</td>
        <td>' + CONVERT(VARCHAR(20),r.start_time,120) + '</td>
        <td>' + r.status + '</td>
        <td>' + r.command + '</td>
        <td>' + CAST(CONVERT(NUMERIC(6,2),r.percent_complete) AS VARCHAR(10)) + '</td>
        <td>' + DB_NAME(r.database_id) + '</td>
        <td>' + CONVERT(VARCHAR(20),DATEADD(ms,r.estimated_completion_time,GETDATE()),120) + '</td>
        <td>' + CAST(CONVERT(NUMERIC(10,2),r.total_elapsed_time/1000.0/60.0) AS VARCHAR(20)) + '</td>
        <td>' + CAST(CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0) AS VARCHAR(20)) + '</td>
        <td>' + CAST(CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0/60.0) AS VARCHAR(20)) + '</td>
        <td>' +
        ISNULL(
            (
                SELECT SUBSTRING
                (
                    text,
                    r.statement_start_offset/2,
                    CASE
                        WHEN r.statement_end_offset=-1
                        THEN LEN(text)
                        ELSE (r.statement_end_offset-r.statement_start_offset)/2
                    END
                )
                FROM sys.dm_exec_sql_text(r.sql_handle)
            )
        ,'') +
        '</td>
        </tr>'
    FROM sys.dm_exec_requests r
    WHERE r.command LIKE '%BACKUP%';

    SET @Body = @Body + '</table>';

    -------------------------------------------------------------------
    -- RDS Task Status Table
    -------------------------------------------------------------------
    SET @Body = @Body +
    '<br><br><h3>RDS Task Details</h3>
     <table border=1 cellspacing=0 cellpadding=4>
     <tr bgcolor="#D9EAD3">
        <th>task_id</th>
        <th>task_type</th>
        <th>database_name</th>
        <th>percent_complete</th>
        <th>duration_mins</th>
        <th>lifecycle</th>
        <th>last_updated</th>
     </tr>';

    SELECT
        @Body = @Body +
        '<tr>
        <td>' + CAST(task_id AS VARCHAR(20)) + '</td>
        <td>' + ISNULL(task_type,'') + '</td>
        <td>' + ISNULL(database_name,'') + '</td>
        <td>' + CAST(percent_complete AS VARCHAR(10)) + '</td>
        <td>' + CAST(duration_mins AS VARCHAR(10)) + '</td>
        <td>' + ISNULL(lifecycle,'') + '</td>
        <td>' + CONVERT(VARCHAR(20),last_updated,120) + '</td>
        </tr>'
    FROM dbo.taskstatusresults_new;

    SET @Body = @Body + '</table></body></html>';

    -------------------------------------------------------------------
    -- Send Mail
    -------------------------------------------------------------------
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = 'DBA_Profile',
        @recipients = 'dba_team@company.com',
        @subject = @Subject,
        @body = @Body,
        @body_format = 'HTML';

END
GO