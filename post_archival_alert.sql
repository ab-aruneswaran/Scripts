USE [DBADB]
GO
/****** Object:  StoredProcedure [dbo].[usp_SendArchivalPostAlert]    Script Date: 13-01-2026 16:59:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[usp_SendArchivalPostAlert]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @HTML NVARCHAR(MAX),
        @MailSubject NVARCHAR(200),
        @DBName SYSNAME = 'AdventureWorks2022';

    ---------------------------------------------------------
    -- 1. Set Email Subject
    ---------------------------------------------------------
    SET @MailSubject = @DBName + ' – Post-Archival Summary Report';

    ---------------------------------------------------------
    -- 2. Build HTML Header (SAME COLOR FORMAT AS PRE-ARCHIVAL)
    ---------------------------------------------------------
    SET @HTML = N'
    <h2 style="color:#1F618D;">Post-Archival Summary Report</h2>
    <h3>Database: <span style="color:#1A5276;">' + @DBName + '</span></h3>

    <table border="1" cellpadding="6" cellspacing="0"
           style="border-collapse:collapse; font-family:Arial; font-size:12px;">
        <tr style="background-color:#1F618D; color:white; font-weight:bold;">
            <th>Sl.No</th>
            <th>Source Table</th>
            <th>Archive Table</th>
            <th>Filter Applied</th>
            <th>Total Rows Archived</th>
            <th>Last Updated</th>
        </tr>';

    ---------------------------------------------------------
    -- 3. Read values from TBL_RETENTION_MASTER
    ---------------------------------------------------------
    DECLARE 
        @SourceTable NVARCHAR(200),
        @DestTable NVARCHAR(200),
        @Filter NVARCHAR(MAX),
        @Rows BIGINT,
        @Status VARCHAR(50),
        @LastUpdated DATETIME,
        @Index INT = 1;

    DECLARE cur CURSOR FOR
    SELECT 
        SourceTablename,
        DestinationTableName,
        Filter,
        [Rows],
        LastUpdated
    FROM DBADB.dbo.TBL_RETENTION_MASTER
    WHERE CAST(LastUpdated AS DATE) = CAST(GETDATE() AS DATE)
    ORDER BY LastUpdated DESC;

    OPEN cur;
    FETCH NEXT FROM cur 
        INTO @SourceTable, @DestTable, @Filter, @Rows, @LastUpdated;

    ---------------------------------------------------------
    -- 4. Loop & Append Rows (SAME COLOR FORMAT)
    ---------------------------------------------------------
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @RowColor NVARCHAR(20) =
            CASE WHEN @Index % 2 = 0 THEN '#D6EAF8' ELSE '#EBF5FB' END;

        SET @HTML += '
        <tr style="background-color:' + @RowColor + '; ">
            <td>' + CAST(@Index AS NVARCHAR(10)) + '</td>
            <td>' + @SourceTable + '</td>
            <td>' + @DestTable + '</td>
            <td>' + ISNULL(@Filter, 'No Filter') + '</td>
            <td>' + CAST(ISNULL(@Rows, 0) AS NVARCHAR(20)) + '</td>
            <td>' + CONVERT(VARCHAR(19), @LastUpdated, 120) + '</td>
        </tr>';

        SET @Index += 1;

        FETCH NEXT FROM cur 
            INTO @SourceTable, @DestTable, @Filter, @Rows, @LastUpdated;
    END

    CLOSE cur;
    DEALLOCATE cur;

    SET @HTML += '</table>';

    ---------------------------------------------------------
    -- 5. Send Email
    ---------------------------------------------------------
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = 'SQL_Training',
        @recipients = 'aruneswaran@geopits.com',
        @subject = @MailSubject,
        @body = @HTML,
        @body_format = 'HTML';

    PRINT 'Post-Archival summary email sent.';
END;


USE msdb;
GO

-- Replace 'YourJobName' with the exact name of your job
EXEC sp_start_job @job_name = 'YourJobName';
GO
