WITH t AS (  
  SELECT 1738238834061 as ts, uuidv7_from_epoch_ms(ts) AS generated_uuid  
)  
SELECT  
  (epoch_ms_from_uuidv7(generated_uuid) = ts) AS is_valid  
FROM t;
