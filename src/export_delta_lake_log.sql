WITH json_data AS (
    -- first convert struct columns to json so all columns have same type
    SELECT columns(*)::json FROM log_json
)
-- only one of the columns in table has non-null value, use it
-- then create jsonl by joining with \n
SELECT string_agg(coalesce(*columns(*)), E'\n') FROM json_data;