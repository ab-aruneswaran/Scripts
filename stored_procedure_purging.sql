USE [DBADB]
GO
/****** Object:  StoredProcedure [dbo].[BankStatementTransactionDetails]    Script Date: 13-01-2026 16:22:52 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER   PROCEDURE [dbo].[BankStatementTransactionDetails]
AS
BEGIN
    SET NOCOUNT ON;

    --===============================
    -- 1. Declare Variables
    --===============================
    DECLARE @sSQL NVARCHAR(MAX);
    DECLARE @SourceTableName SYSNAME = ' [DBLoanguard].[dbo].[BankStatementTransactionDetails]';
    DECLARE @DestinationTableName SYSNAME = '[DBLoanguardHistory].[dbo].[BankStatementTransactionDetails]';
    
	DECLARE @filter NVARCHAR(MAX) ='CreatedOn < DATEADD(MONTH, -3, CAST(GETDATE() AS DATE))'; 
	DECLARE @column NVARCHAR(MAX) ='CreatedOn';
	
    DECLARE @ExecutionId UNIQUEIDENTIFIER = NEWID(); -- Unique run ID
    DECLARE @filter_log NVARCHAR(MAX) = @filter + ' | RunId=' + CAST(@ExecutionId AS NVARCHAR(36));

    DECLARE @ID INT;                     -- New IID_NEW for this run
    DECLARE @rCount BIGINT;              
    DECLARE @TotalrCount BIGINT = 0;
    DECLARE @BatchSize INT = 100000;
    DECLARE @MaxBatchSize INT = 1000000;
    DECLARE @starttime DATETIME;
    DECLARE @endtime DATETIME;

    --===============================
    -- 2. Insert New Master Row
    --===============================
    INSERT INTO TBL_RETENTION_MASTER (SourceTableName, DestinationTableName, Filter, LastUpdated)
    VALUES (@SourceTableName, @DestinationTableName, @filter_log, GETDATE());

    -- Capture new row ID for this execution
    SET @ID = SCOPE_IDENTITY();
    PRINT 'Processing IID_NEW: ' + CAST(@ID AS VARCHAR);

    --===============================
    -- 3. Archival Loop
    --===============================
    UPDATE_BATCH:
    SET @starttime = GETDATE();

    BEGIN TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	DECLARE @RowCount INT;

    -- Build dynamic SQL for batch archival using CTE with READPAST

	SET @sSQL = 
N'SET IDENTITY_INSERT ' + @DestinationTableName + N' ON;

;WITH CTE_ToDelete AS (
    SELECT TOP (' + CAST(@BatchSize AS NVARCHAR(10)) + N') 
[SrNo],[BankStatementID],[TransactionDate],[TransactionValueDate],[TransactionNarration],[TransactionTypeDrAmount],
[TransactionTypeCrAmount],[TransactionTypeDrCrAmount],[TransactionTypeDrCr],[BalanceAmount],[ChequeNo],[ReferenceNo],
[RuleID],[PageNo],[IPAddress],[FontSize],[FontFamily],[IsPredicted],[CreatedOn]
    FROM ' + @SourceTableName + N' WITH (READPAST, ROWLOCK)
    WHERE ' + @filter + N'
    ORDER BY ' + @column + N'
)
DELETE FROM CTE_ToDelete
OUTPUT 
	Deleted.[SrNo],
	Deleted.[BankStatementID],
	Deleted.[TransactionDate],
	Deleted.[TransactionValueDate],
	Deleted.[TransactionNarration],
	Deleted.[TransactionTypeDrAmount],
	Deleted.[TransactionTypeCrAmount],
	Deleted.[TransactionTypeDrCrAmount],
	Deleted.[TransactionTypeDrCr],
	Deleted.[BalanceAmount],
	Deleted.[ChequeNo],
	Deleted.[ReferenceNo],
	Deleted.[RuleID],
	Deleted.[PageNo],
	Deleted.[IPAddress],
	Deleted.[FontSize],
	Deleted.[FontFamily],
	Deleted.[IsPredicted],
	Deleted.[CreatedOn]    
INTO ' + @DestinationTableName + N'(
[SrNo],[BankStatementID],[TransactionDate],[TransactionValueDate],[TransactionNarration],[TransactionTypeDrAmount],
[TransactionTypeCrAmount],[TransactionTypeDrCrAmount],[TransactionTypeDrCr],[BalanceAmount],[ChequeNo],[ReferenceNo],
[RuleID],[PageNo],[IPAddress],[FontSize],[FontFamily],[IsPredicted],[CreatedOn]
    );

	SET @RowCount = @@ROWCOUNT;

	SET IDENTITY_INSERT ' + @DestinationTableName + N' OFF;';

	PRINT @sSQL;
	EXEC sys.sp_executesql @sSQL, N'@RowCount INT OUTPUT', @RowCount = @rCount OUTPUT;
	-- Get number of rows moved in this batch
	PRINT 'Batch Rows Moved: ' + CAST(ISNULL(@rCount, 0) AS VARCHAR(30));

	COMMIT;  -- Commit fast to release locks

    --===============================
    -- 4. Logging
    --===============================
    IF @rCount > 0
    BEGIN
        SET @TotalrCount = @TotalrCount + @rCount;

        -- Log batch archival
        INSERT INTO tbl_Retention_Logs (SourceTableName, DestinationTableName, TotalDataTransfered, Comment, EntryDate)
        VALUES (@SourceTableName, @DestinationTableName, @rCount, 'Successfully - Archived', GETDATE());

        -- Update the master table row for this execution (after commit to avoid blocking SELECT)
        UPDATE TBL_RETENTION_MASTER
        SET LastUpdated = GETDATE(), [Rows] = ISNULL(@TotalrCount, 0)
        WHERE IID_NEW = @ID;
    END
    ELSE
    BEGIN
        -- Log no records found
        INSERT INTO tbl_Retention_Logs (SourceTableName, DestinationTableName, TotalDataTransfered, Comment, EntryDate)
        VALUES (@SourceTableName, @DestinationTableName, 0, 'No Records Found or Already Archived', GETDATE());
    END

    --===============================
    -- 5. Adjust Batch Size Dynamically
    --===============================
    SET @endtime = GETDATE();
    PRINT 'Execution Time: ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(3)) + ' Sec';

    IF DATEDIFF(SECOND, @starttime, @endtime) < 10
    BEGIN
        IF (@BatchSize < @MaxBatchSize)
            SET @BatchSize = @BatchSize + 1000;
    END
    ELSE IF (DATEDIFF(SECOND, @starttime, @endtime) > 10)
    BEGIN
        SET @BatchSize = @BatchSize - 1000;
        IF (@BatchSize < 1000)
            SET @BatchSize = 1000;
    END

    -- Wait to avoid overwhelming live transactions
    WAITFOR DELAY '00:00:01';

    -- Continue if rows still exist
    IF @rCount > 0 GOTO UPDATE_BATCH;

    --===============================
    -- 6. Final Update for This Execution
    --===============================
    UPDATE TBL_RETENTION_MASTER
    SET LastUpdated = GETDATE(),
        [Rows] = ISNULL(@TotalrCount, 0)
    WHERE IID_NEW = @ID;

    PRINT 'Archival Completed Successfully. Total Rows Archived: ' + CAST(@TotalrCount AS VARCHAR(30));
END;
