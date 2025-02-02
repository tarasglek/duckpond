WITH files AS (
  SELECT add.path as added, remove.path as removed
  FROM log_json
)
SELECT added AS file_path
FROM files WHERE added IS NOT NULL
UNION
SELECT removed AS file_path
FROM files WHERE removed IS NOT NULL
