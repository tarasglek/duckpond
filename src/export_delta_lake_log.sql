WITH json_data AS (
    -- first convert struct columns to json so all columns have same type
    select data::json as data from (SELECT * from log_json) data
)
-- only one of the columns in table has non-null value, use it
-- then create jsonl by joining with \n
SELECT string_agg(data, E'\n') FROM json_data;