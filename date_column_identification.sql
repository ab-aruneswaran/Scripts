SELECT  
    s.name  AS SchemaName,
    t.name  AS TableName,
    c.name  AS ColumnName,
    ty.name AS DataType
FROM sys.tables t
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
JOIN sys.columns c 
    ON t.object_id = c.object_id
JOIN sys.types ty 
    ON c.user_type_id = ty.user_type_id
WHERE ty.name IN (
    'date',
    'datetime',
    'datetime2',
    'smalldatetime',
    'time',
    'datetimeoffset'
)
ORDER BY s.name, t.name, c.column_id;

SELECT  
    s.name AS SchemaName,
    t.name AS TableName,
    STRING_AGG(c.name + ' (' + ty.name + ')', ', ') AS DateTimeColumns
FROM sys.tables t
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
JOIN sys.columns c 
    ON t.object_id = c.object_id
JOIN sys.types ty 
    ON c.user_type_id = ty.user_type_id
WHERE ty.name IN (
    'date',
    'datetime',
    'datetime2',
    'smalldatetime',
    'time',
    'datetimeoffset'
)
GROUP BY s.name, t.name
ORDER BY s.name, t.name;
