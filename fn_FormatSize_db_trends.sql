USE [DBADB]
GO

/****** Object:  UserDefinedFunction [dbo].[fn_FormatSize]    Script Date: 04-06-2026 20:34:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   FUNCTION [dbo].[fn_FormatSize]
(
    @SizeMB DECIMAL(38,2)
)
RETURNS VARCHAR(50)
AS
BEGIN
    RETURN
    (
        CASE
            WHEN @SizeMB IS NULL THEN '—'
            WHEN @SizeMB >= 1024
                THEN CAST(CAST(@SizeMB / 1024.0 AS DECIMAL(18,2)) AS VARCHAR(30)) + ' GB'
            ELSE
                CAST(CAST(@SizeMB AS DECIMAL(18,2)) AS VARCHAR(30)) + ' MB'
        END
    );
END;
GO


