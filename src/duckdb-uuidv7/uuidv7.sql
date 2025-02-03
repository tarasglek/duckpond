-- Macro to generate a UUID v7 from a given epoch in milliseconds
CREATE MACRO uuidv7_from_epoch_ms(epoch_ms) AS (
  WITH ts AS (
    SELECT LPAD(TO_HEX(epoch_ms >> 16), 8, '0') AS part1,
           LPAD(TO_HEX(epoch_ms & 65535), 4, '0') AS part2
  ),
  r3 AS (
    SELECT LPAD(TO_HEX(FLOOR(RANDOM() * 4096)::BIGINT),
                3, '0') AS part3_rand
  ),
  r4 AS (
    SELECT LPAD(TO_HEX(FLOOR(RANDOM() * 4096)::BIGINT),
                3, '0') AS part4_rand
  ),
  r5 AS (
    SELECT LPAD(
      TO_HEX(FLOOR(RANDOM() * 281474976710656)::BIGINT),
      12,
      '0'
    ) AS part5_rand
  )
  SELECT CONCAT(
    ts.part1, '-', ts.part2, '-', '7', r3.part3_rand,
    '-', '8', r4.part4_rand, '-', r5.part5_rand
  )
  FROM ts, r3, r4, r5
);

-- Macro to extract epoch (in ms) from a UUID v7 string
CREATE MACRO epoch_ms_from_uuidv7(u) AS (
  SELECT CAST(
    CONCAT(
      '0x',
      CONCAT(SUBSTR(u::text, 1, 8), SUBSTR(u::text, 10, 4))
    )
    AS BIGINT
  )
);

-- Macro to generate a UUID v7 using the current timestamp in ms
CREATE MACRO uuidv7() AS (
  SELECT uuidv7_from_epoch_ms(epoch_ms(CURRENT_TIMESTAMP))
);
