

--Schedule data collection job every one min
-- Ensure the PageLifeExpectancyLog table exists in DBADB
    IF NOT EXISTS (
        SELECT 1 
        FROM DBADB.sys.tables 
        WHERE name = 'PageLifeExpectancyLog' AND schema_id = SCHEMA_ID('dbo')
    )
    BEGIN
        EXEC('
            USE DBADB;
            CREATE TABLE dbo.PageLifeExpectancyLog (
                LogID INT IDENTITY(1,1) PRIMARY KEY,
                CounterName NVARCHAR(255),
                PageLifeExpectancy BIGINT,
                LoggedAt DATETIME DEFAULT GETDATE()
            );
        ');
    END

INSERT INTO DBADB.dbo.PageLifeExpectancyLog (CounterName, PageLifeExpectancy)
SELECT
    [counter_name],
    [cntr_value]
FROM
    sys.dm_os_performance_counters
WHERE
    [object_name] = 'SQLServer:Buffer Manager'
    AND [counter_name] = 'Page life expectancy';
