
--Table Script

USE [DBADB]
GO

/****** Object:  Table [dbo].[TempDBGrowthHistory]    Script Date: 15-09-2026 10:38:18 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TempDBGrowthHistory](
	[CaptureTime] [datetime] NOT NULL,
	[TempDBSizeGB] [decimal](18, 2) NULL
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[TempDBGrowthHistory] ADD  DEFAULT (getdate()) FOR [CaptureTime]
GO



--Job Script 

SET NOCOUNT ON;

DECLARE @CurrentTempDBSizeGB DECIMAL(18,2);
DECLARE @PreviousSizeGB DECIMAL(18,2);
DECLARE @GrowthGB DECIMAL(18,2);

DECLARE @TempDBUsedGB DECIMAL(18,2);
DECLARE @TempDBFreeInsideGB DECIMAL(18,2);

DECLARE @DriveTotalGB DECIMAL(18,2);
DECLARE @DriveFreeGB DECIMAL(18,2);
DECLARE @DriveUsedGB DECIMAL(18,2);

DECLARE @Subject VARCHAR(200);
DECLARE @Body NVARCHAR(MAX);

------------------------------------------------------------
-- Current TempDB Allocated Size
------------------------------------------------------------
SELECT
    @CurrentTempDBSizeGB = SUM(size) * 8.0 / 1024 / 1024
FROM tempdb.sys.database_files;

------------------------------------------------------------
-- Current TempDB Used Space
------------------------------------------------------------
SELECT
    @TempDBUsedGB =
        (
            SUM(user_object_reserved_page_count)
          + SUM(internal_object_reserved_page_count)
          + SUM(version_store_reserved_page_count)
          + SUM(mixed_extent_page_count)
          + SUM(unallocated_extent_page_count)
        ) * 8.0 / 1024 / 1024
FROM tempdb.sys.dm_db_file_space_usage;

SET @TempDBFreeInsideGB = @CurrentTempDBSizeGB - @TempDBUsedGB;

------------------------------------------------------------
-- Drive Information
------------------------------------------------------------
SELECT
    @DriveTotalGB = total_bytes / 1024.0 / 1024 / 1024 ,
    @DriveFreeGB = available_bytes / 1024.0 / 1024 / 1024
FROM sys.dm_os_volume_stats(DB_ID('tempdb'), 1);

SET @DriveUsedGB = @DriveTotalGB - @DriveFreeGB;

------------------------------------------------------------
-- Previous TempDB Size
------------------------------------------------------------
SELECT TOP (1)
    @PreviousSizeGB = TempDBSizeGB
FROM dbo.TempDBGrowthHistory
ORDER BY CaptureTime DESC;

------------------------------------------------------------
-- Save Current Size
------------------------------------------------------------
INSERT INTO dbo.TempDBGrowthHistory
(
    TempDBSizeGB
)
VALUES
(
    @CurrentTempDBSizeGB
);

------------------------------------------------------------
-- Growth Calculation
------------------------------------------------------------
SET @GrowthGB = @CurrentTempDBSizeGB - ISNULL(@PreviousSizeGB,0);

------------------------------------------------------------
-- Send Alert if Growth >= 10 GB
------------------------------------------------------------
IF @PreviousSizeGB IS NOT NULL
AND @GrowthGB >= 10
BEGIN

SET @Body =
'
<html>

<head>

<style>

body
{
    font-family: Arial;
    font-size:12px;
}

table
{
    border-collapse:collapse;
    width:95%;
}

th
{
    background-color:#007ACC;
    color:white;
    border:1px solid black;
    padding:8px;
}

td
{
    border:1px solid black;
    padding:8px;
    text-align:center;
}

</style>

</head>

<body>

<h2>TempDB Growth Alert</h2>

<p>

TempDB has grown by
<b>' + CAST(@GrowthGB AS VARCHAR(20)) + ' GB</b>
during the last monitoring interval on server
<b>' + @@SERVERNAME + '</b>.

</p>

<table>

<tr>

<th>Server</th>
<th>Previous TempDB (GB)</th>
<th>Current TempDB (GB)</th>
<th>Growth (GB)</th>
<th>TempDB Used (GB)</th>
<th>TempDB Free (GB)</th>
<th>Drive Total (GB)</th>
<th>Drive Used (GB)</th>
<th>Drive Free (GB)</th>
<th>Capture Time</th>

</tr>

<tr>

<td>' + @@SERVERNAME + '</td>

<td>' + CAST(@PreviousSizeGB AS VARCHAR(20)) + '</td>

<td>' + CAST(@CurrentTempDBSizeGB AS VARCHAR(20)) + '</td>

<td>' + CAST(@GrowthGB AS VARCHAR(20)) + '</td>

<td>' + CAST(@TempDBUsedGB AS VARCHAR(20)) + '</td>

<td>' + CAST(@TempDBFreeInsideGB AS VARCHAR(20)) + '</td>

<td>' + CAST(@DriveTotalGB AS VARCHAR(20)) + '</td>

<td>' + CAST(@DriveUsedGB AS VARCHAR(20)) + '</td>

<td>' + CAST(@DriveFreeGB AS VARCHAR(20)) + '</td>

<td>' + CONVERT(VARCHAR(19),GETDATE(),120) + '</td>

</tr>

</table>

<br>

<b>Recommendation</b>

<ul>

<li>Review long-running queries using TempDB.</li>

<li>Check index rebuild/reorganize jobs.</li>

<li>Review hash joins and sort operations.</li>

<li>Check version store usage (Snapshot Isolation/RCSI).</li>

<li>Verify sufficient free space remains on the TempDB drive.</li>

</ul>

</body>

</html>';

SET @Subject =
'Invoicemart TEMPDB Growth Alert - ' + @@SERVERNAME;

EXEC msdb.dbo.sp_send_dbmail
      @profile_name = '',
      @recipients   = '',
      @subject      = @Subject,
      @body         = @Body,
      @body_format  = 'HTML';

END;