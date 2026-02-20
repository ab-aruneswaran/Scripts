IF OBJECT_ID('dbo.FileShrinkLog','U') IS NULL
BEGIN
    CREATE TABLE dbo.FileShrinkLog
    (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        DBName SYSNAME,
        FileName SYSNAME,
        FileType VARCHAR(10),
        BeforeSizeMB INT,
        AfterSizeMB INT,
        UsedMB INT,
        ShrinkMB INT,
        TargetFreeMB INT,
        LogDate DATETIME DEFAULT GETDATE()
    )
END
GO

CREATE OR ALTER PROCEDURE dbo.usp_ShrinkFile_WithLogging
(
    @DatabaseName SYSNAME,
    @FileName SYSNAME,
    @ShrinkIncrementMB INT = 10,
    @FreePercent INT = 20
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX)

    SET @SQL = '
    USE ' + QUOTENAME(@DatabaseName) + ';

    DECLARE
        @SizeMB INT,
        @UsedMB INT,
        @TargetFreeMB INT,
        @TargetSizeMB INT,
        @BeforeSizeMB INT

    SELECT @SizeMB = size/128
    FROM sys.database_files
    WHERE name = ''' + @FileName + '''

    IF @SizeMB IS NULL
    BEGIN
        RAISERROR(''File not found'',16,1)
        RETURN
    END

    SELECT @UsedMB = FILEPROPERTY(''' + @FileName + ''',''SpaceUsed'')/128

    WHILE 1=1
    BEGIN
        -- Keep 20% free of used space
        SET @TargetFreeMB = CEILING(@UsedMB * (' + CAST(@FreePercent AS VARCHAR) + ' / 100.0))

        SET @TargetSizeMB = @UsedMB + @TargetFreeMB

        IF @SizeMB <= @TargetSizeMB
            BREAK

        SET @BeforeSizeMB = @SizeMB

        DECLARE @ShrinkToMB INT = @SizeMB - ' + CAST(@ShrinkIncrementMB AS VARCHAR) + '

        DBCC SHRINKFILE (''' + @FileName + ''', @ShrinkToMB)

        SELECT @SizeMB = size/128
        FROM sys.database_files
        WHERE name = ''' + @FileName + '''

        SELECT @UsedMB = FILEPROPERTY(''' + @FileName + ''',''SpaceUsed'')/128

        INSERT INTO DBADB.dbo.FileShrinkLog
        (
            DBName,
            FileName,
            FileType,
            BeforeSizeMB,
            AfterSizeMB,
            UsedMB,
            ShrinkMB,
            TargetFreeMB,
            LogDate
        )
        SELECT
            DB_NAME(),
            ''' + @FileName + ''',
            type_desc,
            @BeforeSizeMB,
            @SizeMB,
            @UsedMB,
            @BeforeSizeMB - @SizeMB,
            @TargetFreeMB,
            GETDATE()
        FROM sys.database_files
        WHERE name = ''' + @FileName + '''
    END
    '

    EXEC (@SQL)
END
GO

EXEC dbo.usp_ShrinkFile_WithLogging
    @DatabaseName = 'ShrinkTestDB',
    @FileName = 'ShrinkTestDB',
    @ShrinkIncrementMB = 10,
    @FreePercent = 20