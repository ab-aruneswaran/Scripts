-- General information about the sql server itself!
SELECT	SERVERPROPERTY('Collation')						AS	[SQL Server Collation],
		SERVERPROPERTY('Edition')						AS	[SQL Server Edition],
		SERVERPROPERTY('ComputerNamePhysicalNetBIOS')	AS	[SQL Server MachineName],
		SERVERPROPERTY('InstanceName')					AS	[SQL Server Instance],
		@@SERVERNAME                   					AS	[SQL Server Name],
		SERVERPROPERTY('IsClustered')					AS	[SQL Server Clustered],
		SERVERPROPERTY('ProductVersion')				AS	[SQL Server Version],
		SERVERPROPERTY('ProductLevel')					AS	[SQL Server Version Level];
GO



-- Information about CPU and Memory
IF CAST(SERVERPROPERTY('ProductVersion') AS CHAR(2)) <= '10'
EXEC	sp_executesql N'
	SELECT	DOSI.cpu_count,
			CAST(DOSI.physical_memory_in_bytes / POWER(1024.0, 3) AS NUMERIC(10, 2))	AS	PhysicalMem_GB,
			CAST(DOSI.bpool_committed / POWER(1024.0, 3) AS NUMERIC(10, 2))				AS	CommittedMem_GB,
			CAST(DOSI.bpool_commit_target / POWER(1024.0, 3) AS NUMERIC(10, 2))			AS	CommittedTargetMem_GB,
			DOSI.max_workers_count
	FROM	sys.dm_os_sys_info AS DOSI;';
ELSE
EXEC	sp_executesql N'
	SELECT	DOSI.cpu_count,
			CAST(DOSI.physical_memory_kb / POWER(1024.0, 2)	AS NUMERIC(10, 2))	AS	PhysicalMem_GB,
			CAST(DOSI.committed_kb / POWER(1024.0, 2) AS NUMERIC(10, 2))		AS	CommittedMem_GB,
			CAST(DOSI.committed_target_kb / POWER(1024.0, 2) AS NUMERIC(10, 2))	AS	CommittedTargetMem_GB,
			DOSI.max_workers_count
	FROM	sys.dm_os_sys_info AS DOSI;';
GO

-- System configuration



SELECT	name,
		description,
		value_in_use,
		is_dynamic,
		C.is_advanced
FROM	master.sys.configurations AS C
WHERE	name IN
(
	N'recovery interval (min)',
	N'locks',
	N'fill factor (%)',
	N'cross db ownership chaining',
	N'max worker threads',
	N'cost threshold for parallelism',
	N'max degree of parallelism',
	N'min server memory (MB)',
	N'max server memory (MB)',
	N'clr enabled',
	N'optimize for ad hoc workloads',
	N'Database Mail XPs',
	N'xp_cmdshell'
);
GO

-- What traceflags are enabled in the sql server environment
-- TF 1118:	uses UNIFORM EXTENTS (8 Pages = 1 Extent)
-- TF 1117: all Files grow at the same time
-- TF 2371:	statistics update threshold will decrease >=25.000
-- IN SQL Server 2016 it is STANDARD!
DBCC TRACESTATUS (-1);
GO

--Database Size 
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO

IF OBJECT_ID('tempdb..##DBSpace') IS NOT NULL
    DROP TABLE ##DBSpace;

CREATE TABLE ##DBSpace
(
    DatabaseName SYSNAME,
    TotalSize_MB DECIMAL(18,2),
    UsedSize_MB DECIMAL(18,2),
    UnusedSize_MB DECIMAL(18,2),
    CompatibilityLevel INT,
    RCSI VARCHAR(3),
    IsTDEEnabled VARCHAR(3),
    FileGroups NVARCHAR(MAX),
    RecoveryModel NVARCHAR(60),
    IsAutoCloseOn VARCHAR(3),
    IsAutoShrinkOn VARCHAR(3),
    Containment NVARCHAR(60),
    DatabaseState NVARCHAR(60)
);

EXEC sp_MSforeachdb '
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;

USE [?];

INSERT INTO ##DBSpace
SELECT
    DB_NAME() AS DatabaseName,

    CAST(SUM(df.size) * 8.0 / 1024 AS DECIMAL(18,2)) AS TotalSize_MB,

    CAST(SUM(FILEPROPERTY(df.name, ''SpaceUsed'')) * 8.0 / 1024 AS DECIMAL(18,2)) AS UsedSize_MB,

    CAST((SUM(df.size) - SUM(FILEPROPERTY(df.name, ''SpaceUsed''))) * 8.0 / 1024 AS DECIMAL(18,2)) AS UnusedSize_MB,

    d.compatibility_level AS CompatibilityLevel,

    CASE
        WHEN d.is_read_committed_snapshot_on = 1 THEN ''Yes''
        ELSE ''No''
    END AS RCSI,

    CASE
        WHEN d.is_encrypted = 1 THEN ''Yes''
        ELSE ''No''
    END AS IsTDEEnabled,

    STUFF
    (
        (
            SELECT '', '' + fg.name
            FROM sys.filegroups fg
            ORDER BY fg.name
            FOR XML PATH(''''), TYPE
        ).value(''.'', ''NVARCHAR(MAX)''),
        1,
        2,
        ''''
    ) AS FileGroups,

    d.recovery_model_desc AS RecoveryModel,

    CASE
        WHEN d.is_auto_close_on = 1 THEN ''Yes''
        ELSE ''No''
    END AS IsAutoCloseOn,

    CASE
        WHEN d.is_auto_shrink_on = 1 THEN ''Yes''
        ELSE ''No''
    END AS IsAutoShrinkOn,

    d.containment_desc AS Containment,

    d.state_desc AS DatabaseState

FROM sys.database_files df
INNER JOIN sys.databases d
    ON d.name = DB_NAME()
GROUP BY
    d.compatibility_level,
    d.is_read_committed_snapshot_on,
    d.is_encrypted,
    d.recovery_model_desc,
    d.is_auto_close_on,
    d.is_auto_shrink_on,
    d.containment_desc,
    d.state_desc;
';

SELECT
    DatabaseName,
    TotalSize_MB,
    UsedSize_MB,
    UnusedSize_MB,
    CAST((UsedSize_MB * 100.0) / NULLIF(TotalSize_MB, 0) AS DECIMAL(5,2)) AS UsedPct,
    CAST((UnusedSize_MB * 100.0) / NULLIF(TotalSize_MB, 0) AS DECIMAL(5,2)) AS FreePct,
    CompatibilityLevel,
    RCSI,
    IsTDEEnabled,
    RecoveryModel,
    IsAutoCloseOn,
    IsAutoShrinkOn,
    Containment,
    DatabaseState,
    FileGroups
FROM ##DBSpace
ORDER BY TotalSize_MB DESC;

DROP TABLE ##DBSpace;
GO
-- TOP 10 tables
;WITH SizeInfo AS
(
    SELECT
        ps.object_id,

        SUM(CASE 
                WHEN ps.index_id IN (0,1) 
                THEN ps.row_count 
                ELSE 0 
            END) AS [Rows],

        CAST(SUM(ps.reserved_page_count) * 8.0 / 1024 AS DECIMAL(18,2)) AS Reserved_MB,

        CAST(
            SUM(
                CASE 
                    WHEN ps.index_id IN (0,1) 
                    THEN ps.in_row_data_page_count 
                         + ps.lob_used_page_count 
                         + ps.row_overflow_used_page_count
                    ELSE ps.lob_used_page_count 
                         + ps.row_overflow_used_page_count
                END
            ) * 8.0 / 1024 AS DECIMAL(18,2)
        ) AS Data_MB,

        CAST(
            (
                SUM(ps.used_page_count) -
                SUM(
                    CASE 
                        WHEN ps.index_id IN (0,1) 
                        THEN ps.in_row_data_page_count 
                             + ps.lob_used_page_count 
                             + ps.row_overflow_used_page_count
                        ELSE ps.lob_used_page_count 
                             + ps.row_overflow_used_page_count
                    END
                )
            ) * 8.0 / 1024 AS DECIMAL(18,2)
        ) AS Index_MB,

        CAST(
            (SUM(ps.reserved_page_count) - SUM(ps.used_page_count)) * 8.0 / 1024 
            AS DECIMAL(18,2)
        ) AS Unused_MB
    FROM sys.dm_db_partition_stats ps
    GROUP BY ps.object_id
),
TableAttributes AS
(
    SELECT
        t.object_id,

        CASE 
            WHEN EXISTS
            (
                SELECT 1
                FROM sys.indexes i
                INNER JOIN sys.data_spaces ds
                    ON i.data_space_id = ds.data_space_id
                WHERE i.object_id = t.object_id
                  AND ds.type = 'PS'
            )
            THEN 'Yes'
            ELSE 'No'
        END AS IsPartitioned,

        ISNULL(
            (
                SELECT MAX(p.partition_number)
                FROM sys.partitions p
                WHERE p.object_id = t.object_id
            ), 0
        ) AS PartitionCount,

        CASE 
            WHEN EXISTS
            (
                SELECT 1
                FROM sys.partitions p
                WHERE p.object_id = t.object_id
                  AND p.data_compression_desc <> 'NONE'
            )
            THEN 'Yes'
            ELSE 'No'
        END AS IsCompressed,

        CASE 
            WHEN EXISTS
            (
                SELECT 1
                FROM sys.partitions p
                WHERE p.object_id = t.object_id
                  AND p.data_compression_desc <> 'NONE'
            )
            THEN
                STUFF(
                    (
                        SELECT DISTINCT ', ' + p2.data_compression_desc
                        FROM sys.partitions p2
                        WHERE p2.object_id = t.object_id
                          AND p2.data_compression_desc <> 'NONE'
                        FOR XML PATH(''), TYPE
                    ).value('.', 'NVARCHAR(MAX)'), 1, 2, ''
                )
            ELSE 'NONE'
        END AS CompressionType
    FROM sys.tables t
)
SELECT
    TOP 10
    DB_NAME() AS DatabaseName,
    s.name AS SchemaName,
    t.name AS TableName,
    si.[Rows],
    si.Reserved_MB,
    si.Data_MB,
    si.Index_MB,
    si.Unused_MB,
    ta.IsPartitioned,
    ta.PartitionCount,
    ta.IsCompressed,
    ta.CompressionType
FROM SizeInfo si
INNER JOIN sys.tables t
    ON si.object_id = t.object_id
INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id
INNER JOIN TableAttributes ta
    ON t.object_id = ta.object_id
ORDER BY
    si.Reserved_MB DESC;

GO

-- Sensitive classification

SELECT
    DB_NAME() AS DatabaseName,
    s.name AS SchemaName,
    t.name AS TableName,
    c.name AS ColumnName,
    ty.name AS DataType,
    c.max_length,
    c.precision,
    c.scale,

    CASE
        WHEN c.name LIKE '%email%' THEN 'Contact Info'
        WHEN c.name LIKE '%mail%' THEN 'Contact Info'

        WHEN c.name LIKE '%mobile%' 
          OR c.name LIKE '%phone%' 
          OR c.name LIKE '%contact%' 
           THEN 'Contact Info'

        WHEN c.name LIKE '%dob%' 
          OR c.name LIKE '%birth%' 
          OR c.name LIKE '%dateofbirth%' THEN 'Date Of Birth'

        WHEN c.name LIKE '%address%' 
          OR c.name LIKE '%addr%' 
          OR c.name LIKE '%city%' 
          OR c.name LIKE '%state%' 
          OR c.name LIKE '%country%' 
          OR c.name LIKE '%pincode%' 
          OR c.name LIKE '%pin_code%' 
          OR c.name LIKE '%zipcode%' 
          OR c.name LIKE '%zip%' THEN 'Contact Info'

        WHEN c.name LIKE '%pan%' 
          OR c.name LIKE '%aadhaar%' 
          OR c.name LIKE '%aadhar%' 
          OR c.name LIKE '%ssn%' 
          OR c.name LIKE '%national%id%' 
          OR c.name LIKE '%passport%' 
          OR c.name LIKE '%voter%' THEN 'National ID'

        WHEN c.name LIKE '%account%' 
          OR c.name LIKE '%bank%' 
          OR c.name LIKE '%ifsc%' 
          OR c.name LIKE '%iban%' 
          OR c.name LIKE '%swift%' THEN 'Banking'

        WHEN c.name LIKE '%card%' 
          OR c.name LIKE '%credit%' 
          OR c.name LIKE '%debit%' 
          OR c.name LIKE '%cvv%' THEN 'Credit Card'

        WHEN c.name LIKE '%password%' 
          OR c.name LIKE '%passwd%' 
          OR c.name LIKE '%pwd%' 
          OR c.name LIKE '%token%' 
          OR c.name LIKE '%secret%' 
          OR c.name LIKE '%apikey%' 
          OR c.name LIKE '%api_key%' THEN 'Credentials'

        WHEN c.name LIKE '%salary%' 
          OR c.name LIKE '%income%' 
          OR c.name LIKE '%amount%' 
          OR c.name LIKE '%payment%' THEN 'Financial'

        ELSE 'Other'
    END AS InformationType,

    CASE
        WHEN c.name LIKE '%password%' 
          OR c.name LIKE '%passwd%' 
          OR c.name LIKE '%pwd%' 
          OR c.name LIKE '%token%' 
          OR c.name LIKE '%secret%' 
          OR c.name LIKE '%apikey%' 
          OR c.name LIKE '%api_key%' THEN 'Highly Confidential'

        WHEN c.name LIKE '%pan%' 
          OR c.name LIKE '%aadhaar%' 
          OR c.name LIKE '%aadhar%' 
          OR c.name LIKE '%ssn%' 
          OR c.name LIKE '%passport%' 
          OR c.name LIKE '%card%' 
          OR c.name LIKE '%cvv%' THEN 'Highly Confidential'

        ELSE 'Confidential'
    END AS RecommendedSensitivityLabel,

    CASE
        WHEN c.name LIKE '%password%' 
          OR c.name LIKE '%passwd%' 
          OR c.name LIKE '%pwd%' 
          OR c.name LIKE '%token%' 
          OR c.name LIKE '%secret%' 
          OR c.name LIKE '%apikey%' 
          OR c.name LIKE '%api_key%' THEN 'CRITICAL'

        WHEN c.name LIKE '%pan%' 
          OR c.name LIKE '%aadhaar%' 
          OR c.name LIKE '%aadhar%' 
          OR c.name LIKE '%ssn%' 
          OR c.name LIKE '%passport%' 
          OR c.name LIKE '%card%' 
          OR c.name LIKE '%cvv%' THEN 'HIGH'

        WHEN c.name LIKE '%email%' 
          OR c.name LIKE '%mobile%' 
          OR c.name LIKE '%phone%' 
          OR c.name LIKE '%dob%' 
          OR c.name LIKE '%birth%' THEN 'MEDIUM'

        ELSE 'LOW'
    END AS RecommendedRank,

    CASE
        WHEN sc.major_id IS NOT NULL THEN 'Yes'
        ELSE 'No'
    END AS AlreadyClassified,

    sc.label AS ExistingLabel,
    sc.information_type AS ExistingInformationType,
    sc.rank_desc AS ExistingRank

FROM sys.tables t
INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id
INNER JOIN sys.columns c
    ON t.object_id = c.object_id
INNER JOIN sys.types ty
    ON c.user_type_id = ty.user_type_id
LEFT JOIN sys.sensitivity_classifications sc
    ON sc.major_id = c.object_id
   AND sc.minor_id = c.column_id
WHERE
       c.name LIKE '%email%'
    OR c.name LIKE '%mail%'
    OR c.name LIKE '%mobile%'
    OR c.name LIKE '%phone%'
    OR c.name LIKE '%contact%'
    OR c.name LIKE '%dob%'
    OR c.name LIKE '%birth%'
    OR c.name LIKE '%address%'
    OR c.name LIKE '%addr%'
    OR c.name LIKE '%city%'
    OR c.name LIKE '%state%'
    OR c.name LIKE '%country%'
    OR c.name LIKE '%pincode%'
    OR c.name LIKE '%pin_code%'
    OR c.name LIKE '%zipcode%'
    OR c.name LIKE '%zip%'
    OR c.name LIKE '%pan%'
    OR c.name LIKE '%aadhaar%'
    OR c.name LIKE '%aadhar%'
    OR c.name LIKE '%ssn%'
    OR c.name LIKE '%national%id%'
    OR c.name LIKE '%passport%'
    OR c.name LIKE '%voter%'
    OR c.name LIKE '%account%'
    OR c.name LIKE '%bank%'
    OR c.name LIKE '%ifsc%'
    OR c.name LIKE '%iban%'
    OR c.name LIKE '%swift%'
    OR c.name LIKE '%card%'
    OR c.name LIKE '%credit%'
    OR c.name LIKE '%debit%'
    OR c.name LIKE '%cvv%'
    OR c.name LIKE '%password%'
    OR c.name LIKE '%passwd%'
    OR c.name LIKE '%pwd%'
    OR c.name LIKE '%token%'
    OR c.name LIKE '%secret%'
    OR c.name LIKE '%apikey%'
    OR c.name LIKE '%api_key%'
    OR c.name LIKE '%salary%'
    OR c.name LIKE '%income%'
    OR c.name LIKE '%amount%'
    OR c.name LIKE '%payment%'
ORDER BY
    s.name,
    t.name,
    c.column_id;
GO

--DDM details

SELECT   s.name AS schema_name,
         t.name AS table_name,
         c.name AS column_name,
         c.masking_function
FROM     sys.masked_columns AS c
         INNER JOIN
         sys.tables AS t
         ON c.object_id = t.object_id
         INNER JOIN
         sys.schemas AS s
         ON t.schema_id = s.schema_id
WHERE    c.is_masked = 1
ORDER BY s.name, t.name, c.name;


--Overlapped & Duplicate Indexes

SELECT t1.tablename,
      t1.indexname AS Index1,
      t1.columnlist AS Index1Columns,
      t1.IndexSizeMB AS Index1SizeMB,
      t2.indexname AS Index2,
      t2.columnlist AS Index2Columns,
      t2.IndexSizeMB AS Index2SizeMB
FROM (
   SELECT DISTINCT
          OBJECT_NAME(i.object_id) AS tablename,
          i.name AS indexname,
          CAST(SUM(ps.used_page_count) * 8.0 / 1024 AS DECIMAL(18,2)) AS IndexSizeMB,
          STUFF((
              SELECT ', ' + c.name
              FROM sys.index_columns ic1
              INNER JOIN sys.columns c
                  ON ic1.object_id = c.object_id
                 AND ic1.column_id = c.column_id
              WHERE ic1.object_id = i.object_id
                AND ic1.index_id = i.index_id
              ORDER BY ic1.index_column_id
              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS columnlist
   FROM sys.indexes i
   INNER JOIN sys.objects o
       ON i.object_id = o.object_id
   INNER JOIN sys.dm_db_partition_stats ps
       ON i.object_id = ps.object_id
      AND i.index_id = ps.index_id
   WHERE o.is_ms_shipped = 0
   GROUP BY i.object_id, i.index_id, i.name
) t1
JOIN (
   SELECT DISTINCT
          OBJECT_NAME(i.object_id) AS tablename,
          i.name AS indexname,
          CAST(SUM(ps.used_page_count) * 8.0 / 1024 AS DECIMAL(18,2)) AS IndexSizeMB,
          STUFF((
              SELECT ', ' + c.name
              FROM sys.index_columns ic1
              INNER JOIN sys.columns c
                  ON ic1.object_id = c.object_id
                 AND ic1.column_id = c.column_id
              WHERE ic1.object_id = i.object_id
                AND ic1.index_id = i.index_id
              ORDER BY ic1.index_column_id
              FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS columnlist
   FROM sys.indexes i
   INNER JOIN sys.objects o
       ON i.object_id = o.object_id
   INNER JOIN sys.dm_db_partition_stats ps
       ON i.object_id = ps.object_id
      AND i.index_id = ps.index_id
   WHERE o.is_ms_shipped = 0
   GROUP BY i.object_id, i.index_id, i.name
) t2
ON t1.tablename = t2.tablename
AND t1.indexname <> t2.indexname
AND (t1.columnlist = t2.columnlist OR t2.columnlist LIKE t1.columnlist + ',%')
ORDER BY t1.tablename;


-- Unused Indexes
SELECT
    o.name AS TableName,
    i.name AS IndexName,
    CAST(SUM(ps.used_page_count) * 8.0 / 1024 AS DECIMAL(18,2)) AS IndexSizeMB,
    dm_ius.user_seeks AS UserSeeks,
    dm_ius.last_user_seek AS LastUserSeek,
    dm_ius.user_scans AS UserScans,
    dm_ius.last_user_scan AS LastUserScan,
    dm_ius.user_lookups AS UserLookups,
    dm_ius.last_user_lookup AS LastUserLookup,
    dm_ius.user_updates AS UserUpdates,
    dm_ius.last_user_update AS LastUserUpdate
FROM sys.dm_db_index_usage_stats dm_ius
INNER JOIN sys.indexes i
    ON i.index_id = dm_ius.index_id
   AND dm_ius.object_id = i.object_id
INNER JOIN sys.objects o
    ON dm_ius.object_id = o.object_id
INNER JOIN sys.dm_db_partition_stats ps
    ON i.object_id = ps.object_id
   AND i.index_id = ps.index_id
WHERE
    OBJECTPROPERTY(dm_ius.object_id, 'IsUserTable') = 1
    AND i.is_primary_key = 0
    AND i.is_unique = 0
    AND dm_ius.user_seeks = 0
    AND dm_ius.user_scans = 0
    AND dm_ius.user_lookups = 0
GROUP BY
    o.name,
    i.name,
    dm_ius.user_seeks,
    dm_ius.last_user_seek,
    dm_ius.user_scans,
    dm_ius.last_user_scan,
    dm_ius.user_lookups,
    dm_ius.last_user_lookup,
    dm_ius.user_updates,
    dm_ius.last_user_update
ORDER BY
    IndexSizeMB DESC;
