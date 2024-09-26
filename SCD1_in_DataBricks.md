## SCD type 1 using Merge into statement

```
MERGE INTO TargetTable AS target
USING SourceTable AS source
ON target.ID = source.ID
WHEN MATCHED THEN
    UPDATE SET 
        target.Col1 = source.Col1,
        target.Col2 = source.Col2
WHEN NOT MATCHED THEN
    INSERT (ID, Col1, Col2)
    VALUES (source.ID, source.Col1, source.Col2);
```
