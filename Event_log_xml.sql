USE [DBADB]
GO
Create PROCEDURE [dbo].[usp_Send_ErrorLogs_Email]
AS
BEGIN
SET NOCOUNT ON;
DECLARE @ClientName VARCHAR(100) = 'Cropin-Node1'
DECLARE @ServerName VARCHAR(100) = @@SERVERNAME
DECLARE @DBType VARCHAR(20) = 'MSSQL'
DECLARE @Subject VARCHAR(300)
DECLARE @Body NVARCHAR(MAX)
SET @Subject =
@ClientName + ' | ' + @ServerName + ' | ' + @DBType + ' | Error Logs - Last 1 Hour'
DECLARE @ErrorLogs TABLE
(
 LogDate DATETIME,
 ProcessInfo NVARCHAR(100),
 Text NVARCHAR(MAX)
)
INSERT INTO @ErrorLogs
EXEC xp_readerrorlog 0,1
SELECT @Body =
(
SELECT
LogDate,
ProcessInfo,
Text
FROM @ErrorLogs
WHERE LogDate >= DATEADD(HOUR,-1,GETDATE())
FOR XML PATH('log'), ROOT('ErrorLogs')
)
IF @Body IS NULL
SET @Body = 'No errors found in last hour.'
EXEC msdb.dbo.sp_send_dbmail
@profile_name = 'Amazon Ireland SES AccounT',
@recipients = 'dccagent@geopits.com',
@subject = @Subject,
@body = @Body,
@body_format = 'TEXT'
END
2.Event Logs
CREATE PROCEDURE [dbo].[usp_Send_EventLogs_Email]
AS
BEGIN
 SET NOCOUNT ON;
 DECLARE @ClientName VARCHAR(100) = 'Cropin-Node1'
 DECLARE @ServerName VARCHAR(100) = @@SERVERNAME
 DECLARE @DBType VARCHAR(50) = 'MSSQL'
 DECLARE @Subject NVARCHAR(300)
 DECLARE @Body NVARCHAR(MAX)
 SET @Subject =
 @ClientName + ' | ' + @ServerName + ' | ' + @DBType + ' | Windows Event Logs (Last 1 
Hour)'
 CREATE TABLE #EventLogs
 (
 Message NVARCHAR(MAX)
 )
 INSERT INTO #EventLogs (Message)
 EXEC xp_cmdshell 'powershell -command "Get-WinEvent -FilterHashtable 
@{LogName=''Application''; StartTime=(Get-Date).AddHours(-1)} | Where-Object 
{$_.ProviderName -like ''*SQL*''} | Select-Object TimeCreated, ProviderName, Message | 
ConvertTo-Csv -NoTypeInformation"'
 CREATE TABLE #ParsedEventLogs
 (
 TimeCreated DATETIME,
 ProviderName NVARCHAR(255),
 Message NVARCHAR(MAX)
 )
 DECLARE @CsvData NVARCHAR(MAX)
 DECLARE @Line NVARCHAR(MAX)
 DECLARE @TimeCreated NVARCHAR(255)
 DECLARE @ProviderName NVARCHAR(255)
 DECLARE @Message NVARCHAR(MAX)
 -- Replace STRING_AGG
 SELECT @CsvData =
 STUFF((
 SELECT CHAR(13) + CHAR(10) + Message
 FROM #EventLogs
 FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
 ,1,2,'')
 WHILE CHARINDEX(CHAR(13), @CsvData) > 0
 BEGIN
 SET @Line = LTRIM(RTRIM(SUBSTRING(@CsvData, 1, CHARINDEX(CHAR(13), @CsvData) 
- 1)))
 SET @CsvData = SUBSTRING(@CsvData, CHARINDEX(CHAR(13), @CsvData) + 2, 
LEN(@CsvData))
 SET @TimeCreated = PARSENAME(REPLACE(@Line, ',', '.'), 3)
 SET @ProviderName = PARSENAME(REPLACE(@Line, ',', '.'), 2)
 SET @Message = PARSENAME(REPLACE(@Line, ',', '.'), 1)
 BEGIN TRY
 INSERT INTO #ParsedEventLogs (TimeCreated, ProviderName, Message)
 VALUES (
 TRY_CONVERT(DATETIME, @TimeCreated),
 @ProviderName,
 @Message
 )
 END TRY
 BEGIN CATCH
 PRINT 'Failed to convert TimeCreated: ' + @TimeCreated
 END CATCH
 END
 -- Replace STRING_AGG for email body
 SELECT @Body =
 STUFF((
 SELECT
 CHAR(13) +
 'Time: ' + CONVERT(VARCHAR, TimeCreated, 120) + CHAR(13) +
 'Provider: ' + ProviderName + CHAR(13) +
 'Message: ' + Message + CHAR(13) +
 '-------------------------------------------------' + CHAR(13)
 FROM #ParsedEventLogs
 FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
 ,1,1,'')
 IF @Body IS NULL OR LEN(@Body) = 0
 SET @Body = 'No Windows event logs related to SQL Server in the last 1 hour.'
 EXEC msdb.dbo.sp_send_dbmail
 @profile_name = 'Amazon Ireland SES AccounT',
 @recipients = 'dccagent@geopits.com',
 @subject = @Subject,
 @body = @Body,
 @body_format = 'TEXT'
 DROP TABLE #EventLogs
 DROP TABLE #ParsedEventLogs
END
3. Agent Logs
USE [DBADB]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create PROCEDURE [dbo].[usp_Send_AgentJobLogs_Email]
AS
BEGIN
SET NOCOUNT ON;
DECLARE @ClientName VARCHAR(100) = 'Cropin-Node2'
DECLARE @ServerName VARCHAR(100) = @@SERVERNAME
DECLARE @DBType VARCHAR(50) = 'MSSQL'
DECLARE @Subject NVARCHAR(300)
DECLARE @Body NVARCHAR(MAX)
SET @Subject =
@ClientName + ' | ' + @ServerName + ' | ' + @DBType + ' | SQL Agent Job Logs (Last 1 Hour)'
DECLARE @JobLogs TABLE
(
JobName NVARCHAR(200),
StepName NVARCHAR(200),
RunDateTime DATETIME,
RunStatus VARCHAR(50),
Message NVARCHAR(MAX)
)
INSERT INTO @JobLogs
SELECT
j.name AS JobName,
s.step_name AS StepName,
msdb.dbo.agent_datetime(h.run_date, h.run_time) AS RunDateTime,
CASE h.run_status
 WHEN 0 THEN 'FAILED'
 WHEN 1 THEN 'SUCCEEDED'
 WHEN 2 THEN 'RETRY'
 WHEN 3 THEN 'CANCELLED'
 WHEN 4 THEN 'IN PROGRESS'
END AS RunStatus,
h.message
FROM msdb.dbo.sysjobhistory h
JOIN msdb.dbo.sysjobs j
ON h.job_id = j.job_id
LEFT JOIN msdb.dbo.sysjobsteps s
ON h.job_id = s.job_id
AND h.step_id = s.step_id
WHERE msdb.dbo.agent_datetime(h.run_date, h.run_time)
>= DATEADD(HOUR,-1,GETDATE())
AND h.step_id > 0 -- exclude summary row
ORDER BY RunDateTime DESC
SELECT @Body =
(
SELECT
JobName,
StepName,
RunDateTime,
RunStatus,
Message
FROM @JobLogs
FOR JSON PATH, ROOT('AgentJobLogs')
)
IF @Body IS NULL
SET @Body = 'No SQL Agent job events in the last 1 hour.'
EXEC msdb.dbo.sp_send_dbmail
@profile_name='Amazon Ireland SES AccounT',
@recipients='dccagent@geopits.com',
@subject=@Subject,
@body=@Body,
@body_format='TEXT'
END
JOB Creation
Step1: exec usp_Send_ErrorLogs_Email
Step2: exec usp_Send_EventLogs_Email
Step 3: exec usp_Send_AgentJobLogs_Email
Job Shedule: every one hour - dail