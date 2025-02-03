## Duckdb uuidv7 implementation

The following demonstrates how to load and test the UUID v7 macros in the DuckDB CLI.

Inside the DuckDB CLI, load the macros file by running:

```sql
.read uuidv7.sql
```

This command will execute the SQL in "uuidv7.sql" and create the macros for use. Once loaded, you can use the macros as follows:

```sql
-- Test all macros
SELECT
  uuidv7_from_epoch_ms(1738238834061) AS generated_uuid,
  epoch_ms_from_uuidv7(uuidv7_from_epoch_ms(1738238834061)) AS recovered_epoch,
  uuidv7() AS current_uuid;
```

These were generated using o3-mini in https://chatcraft.org/api/share/tarasglek/rbGG86Pqevz0igzHlQ4Nv
