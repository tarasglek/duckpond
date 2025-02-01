WITH files AS (
  SELECT 
    add.path AS added, 
    remove.path AS removed 
  FROM log_json
)
SELECT added
FROM files
WHERE 
  added is NOT NULL
  AND added NOT IN (
    SELECT removed 
    FROM files 
    WHERE removed IS NOT NULL
  )
