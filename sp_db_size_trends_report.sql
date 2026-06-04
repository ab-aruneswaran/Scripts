USE DBADB;
GO
--Client Name in Subject Line Number 34
--Database Mail Profile Line Number 195
CREATE OR ALTER PROCEDURE dbo.usp_Send_DB_TableSize_Report
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EmailBody   NVARCHAR(MAX);
    DECLARE @Subject     NVARCHAR(500);
    DECLARE @Recipients  NVARCHAR(500) = 'dccagent@geopits.com';  -- ← change
    DECLARE @CC          NVARCHAR(500) = 'mssqltechsupport@geopits.com'; -- ← change
    DECLARE @Today       DATE = CAST(GETDATE() AS DATE);

    -- =========================================================
    -- Get last 5 distinct dates from DB_Meta
    -- =========================================================
    SELECT TOP 5 CAST([Dates] AS DATE) AS ReportDate
    INTO #Dates
    FROM [DBADB].[dbo].[DB_Meta]
    GROUP BY CAST([Dates] AS DATE)
    ORDER BY CAST([Dates] AS DATE) DESC;

    DECLARE @D1 DATE, @D2 DATE, @D3 DATE, @D4 DATE, @D5 DATE;
    SELECT @D5 = MIN(ReportDate), @D1 = MAX(ReportDate) FROM #Dates;
    SELECT @D2 = MAX(ReportDate) FROM #Dates WHERE ReportDate < @D1;
    SELECT @D3 = MAX(ReportDate) FROM #Dates WHERE ReportDate < @D2;
    SELECT @D4 = MAX(ReportDate) FROM #Dates WHERE ReportDate < @D3;

    -- =========================================================
    -- Build CSS + Header
    -- =========================================================
    SET @Subject = 'Pepper Advantage SQL Server DB & Table Size Report | ' 
                   + CONVERT(VARCHAR(10), @Today, 105);  -- DD-MM-YYYY

    SET @EmailBody = N'
<html><head><meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<style>
  body { font-family: "Segoe UI", Arial, sans-serif; font-size:13px; color:#0f172a; background-color:#f0f9ff; }
  h2   { color:#075985; }
  h3   { color:#0369a1; }
  table{ border-collapse:collapse; width:100%; margin-bottom:30px;
         background-color:#ffffff; border-radius:6px;
         box-shadow:0 3px 10px rgba(2,132,199,0.15); }
  th   { color:#075985; padding:10px; font-weight:700; border:1px solid #7dd3fc;
         text-transform:uppercase; letter-spacing:0.3px; }
  td   { border:1px solid #bae6fd; padding:8px; font-weight:600; }
  tr:hover td { background-color:#e0f2fe; }
  .today { font-weight:800; color:#065f46; }
  .summary { margin-bottom:20px; padding:12px 15px;
             border-left:5px solid #0ea5e9; border-radius:5px; }
  .footer  { margin-top:40px; font-size:11px; color:#64748b; text-align:center; }
</style></head><body>
<h2>SQL Server DB and Table Size Trend Report</h2>
<div class="summary">
  <b>Generated On:</b> ' + CONVERT(VARCHAR(30), GETDATE(), 120) + '<br>
  <b>Instance:</b> ' + @@SERVERNAME + '<br>
</div>';

    -- =========================================================
    -- Section 1: Top 10 Databases
    -- =========================================================
    SET @EmailBody += N'<h3>Top 10 Databases (by latest size)</h3>
<table><tr>
  <th>Instance</th><th>Database</th>
  <th>' + CONVERT(VARCHAR(10),@D5,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D4,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D3,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D2,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D1,105) + '</th>
</tr>';

    SELECT @EmailBody += N'<tr>
      <td><b>' + ISNULL(d1.[Instance_Name],'') + '</b></td>
      <td><b>' + ISNULL(d1.[Database_Names],'') + '</b></td>
      <td>' + dbo.fn_FormatSize(d5.[Size MB]) + '</td>
<td>' + dbo.fn_FormatSize(d4.[Size MB]) + '</td>
<td>' + dbo.fn_FormatSize(d3.[Size MB]) + '</td>
<td>' + dbo.fn_FormatSize(d2.[Size MB]) + '</td>
<td class="today">' + dbo.fn_FormatSize(d1.[Size MB]) + '</td>
    </tr>'
    FROM (
        SELECT TOP 10 [Instance_Name],[Database_Names],[Size MB]
        FROM [DBADB].[dbo].[DB_Meta]
        WHERE CAST([Dates] AS DATE) = @D1 AND Database_Names NOT IN ('master','DBADB','model','msdb','tempdb','ReportServerTempDB') 
        ORDER BY [Size MB] DESC
    ) d1
    LEFT JOIN [DBADB].[dbo].[DB_Meta] d2
        ON d2.[Database_Names]=d1.[Database_Names] AND CAST(d2.[Dates] AS DATE)=@D2
    LEFT JOIN [DBADB].[dbo].[DB_Meta] d3
        ON d3.[Database_Names]=d1.[Database_Names] AND CAST(d3.[Dates] AS DATE)=@D3
    LEFT JOIN [DBADB].[dbo].[DB_Meta] d4
        ON d4.[Database_Names]=d1.[Database_Names] AND CAST(d4.[Dates] AS DATE)=@D4
    LEFT JOIN [DBADB].[dbo].[DB_Meta] d5
        ON d5.[Database_Names]=d1.[Database_Names] AND CAST(d5.[Dates] AS DATE)=@D5;

    SET @EmailBody += N'</table>';

    -- =========================================================
    -- Section 2: Top 10 Tables
    -- =========================================================
    SET @EmailBody += N'<h3>Top 10 Tables (by latest size)</h3>
<table><tr>
  <th>Database</th><th>Schema</th><th>Table</th>
  <th>' + CONVERT(VARCHAR(10),@D5,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D4,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D3,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D2,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D1,105) + '</th>
</tr>';

    SELECT @EmailBody += N'<tr>
      <td><b>' + ISNULL(t1.[DBNAME],'') + '</b></td>
      <td><b>' + ISNULL(t1.[SchemaName],'') + '</b></td>
      <td><b>' + ISNULL(t1.[TableName],'') + '</b></td>
      <td>' + dbo.fn_FormatSize(t5.[TotalSpaceMB]) + '</td>
<td>' + dbo.fn_FormatSize(t4.[TotalSpaceMB]) + '</td>
<td>' + dbo.fn_FormatSize(t3.[TotalSpaceMB]) + '</td>
<td>' + dbo.fn_FormatSize(t2.[TotalSpaceMB]) + '</td>
<td class="today">' + dbo.fn_FormatSize(t1.[TotalSpaceMB]) + '</td>
    </tr>'
    FROM (
        SELECT TOP 10 [DBID],[DBNAME],[TableName],[SchemaName],[TotalSpaceMB]
        FROM [DBADB].[dbo].[TableSizeData]
        WHERE CAST([Date] AS DATE) = @D1 AND DBNAME NOT IN ('master','DBADB','model','msdb','tempdb','ReportServerTempDB')
        ORDER BY [TotalSpaceMB] DESC
    ) t1
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t2
        ON t2.[DBNAME]=t1.[DBNAME] AND t2.[TableName]=t1.[TableName]
           AND t2.[SchemaName]=t1.[SchemaName] AND CAST(t2.[Date] AS DATE)=@D2
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t3
        ON t3.[DBNAME]=t1.[DBNAME] AND t3.[TableName]=t1.[TableName]
           AND t3.[SchemaName]=t1.[SchemaName] AND CAST(t3.[Date] AS DATE)=@D3
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t4
        ON t4.[DBNAME]=t1.[DBNAME] AND t4.[TableName]=t1.[TableName]
           AND t4.[SchemaName]=t1.[SchemaName] AND CAST(t4.[Date] AS DATE)=@D4
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t5
        ON t5.[DBNAME]=t1.[DBNAME] AND t5.[TableName]=t1.[TableName]
           AND t5.[SchemaName]=t1.[SchemaName] AND CAST(t5.[Date] AS DATE)=@D5;

    SET @EmailBody += N'</table>';

    -- =========================================================
    -- Section 3: All Tables
    -- =========================================================
    SET @EmailBody += N'<h3>All Tables</h3>
<table><tr>
  <th>Database</th><th>Schema</th><th>Table</th><th>Rows</th>
  <th>' + CONVERT(VARCHAR(10),@D5,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D4,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D3,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D2,105) + '</th>
  <th>' + CONVERT(VARCHAR(10),@D1,105) + '</th>
</tr>';

    SELECT @EmailBody += N'<tr>
      <td><b>' + ISNULL(t1.[DBNAME],'') + '</b></td>
      <td><b>' + ISNULL(t1.[SchemaName],'') + '</b></td>
      <td><b>' + ISNULL(t1.[TableName],'') + '</b></td>
      <td>' + ISNULL(CAST(t1.[rows] AS VARCHAR(20)),'—') + '</td>
      <td>' + dbo.fn_FormatSize(t5.[TotalSpaceMB]) + '</td>
<td>' + dbo.fn_FormatSize(t4.[TotalSpaceMB]) + '</td>
<td>' + dbo.fn_FormatSize(t3.[TotalSpaceMB]) + '</td>
<td>' + dbo.fn_FormatSize(t2.[TotalSpaceMB]) + '</td>
<td class="today">' + dbo.fn_FormatSize(t1.[TotalSpaceMB]) + '</td>
    </tr>'
    FROM (
        SELECT [DBID],[DBNAME],[TableName],[SchemaName],[rows],[TotalSpaceMB]
        FROM [DBADB].[dbo].[TableSizeData]
        WHERE CAST([Date] AS DATE) = @D1 AND DBNAME NOT IN ('master','DBADB','model','msdb','tempdb','ReportServerTempDB')
    ) t1
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t2
        ON t2.[DBNAME]=t1.[DBNAME] AND t2.[TableName]=t1.[TableName]
           AND t2.[SchemaName]=t1.[SchemaName] AND CAST(t2.[Date] AS DATE)=@D2
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t3
        ON t3.[DBNAME]=t1.[DBNAME] AND t3.[TableName]=t1.[TableName]
           AND t3.[SchemaName]=t1.[SchemaName] AND CAST(t3.[Date] AS DATE)=@D3
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t4
        ON t4.[DBNAME]=t1.[DBNAME] AND t4.[TableName]=t1.[TableName]
           AND t4.[SchemaName]=t1.[SchemaName] AND CAST(t4.[Date] AS DATE)=@D4
    LEFT JOIN [DBADB].[dbo].[TableSizeData] t5
        ON t5.[DBNAME]=t1.[DBNAME] AND t5.[TableName]=t1.[TableName]
           AND t5.[SchemaName]=t1.[SchemaName] AND CAST(t5.[Date] AS DATE)=@D5
    ORDER BY t1.[TotalSpaceMB] DESC;

    SET @EmailBody += N'</table>
<div class="footer">Automated Report - SQL Server DBA</div>
</body></html>';

    -- =========================================================
    -- Send the email
    -- =========================================================
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name  = 'DBA',
        @recipients    = @Recipients,
        @copy_recipients = @CC,
        @subject       = @Subject,
        @body          = @EmailBody,
        @body_format   = 'HTML';

    DROP TABLE IF EXISTS #Dates;
END;
GO

EXEC DBADB.dbo.usp_Send_DB_TableSize_Report;

