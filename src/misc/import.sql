 INSERT INTO schema_log
 SELECT rows.*
 FROM (
     SELECT unnest(schema_log) AS rows
     FROM 'dump.json'
 ) AS schema_log;

 select * from schema_log;