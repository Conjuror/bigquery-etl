/*

Floor a timestamp object to the given minute interval.

 */
CREATE TEMP FUNCTION udf_round_timestamp_to_minute(timestamp_expression TIMESTAMP, minute INT64) AS (
  TIMESTAMP_SECONDS(
    CAST((FLOOR(UNIX_SECONDS(timestamp_expression) / (minute * 60)) * minute * 60) AS INT64)
  )
);
-- Test
SELECT
  assert_equals(
    TIMESTAMP '2019-01-01 13:30:00',
    udf_round_timestamp_to_minute(TIMESTAMP '2019-01-01 13:31:11', 5)
  ),
  assert_equals(
    TIMESTAMP '2019-05-03 00:30:00',
    udf_round_timestamp_to_minute(TIMESTAMP '2019-05-03 00:34:59', 5)
  ),
  assert_equals(
    TIMESTAMP '2019-05-03 00:34:00',
    udf_round_timestamp_to_minute(TIMESTAMP '2019-05-03 00:34:59', 2)
  ),
  assert_equals(
    TIMESTAMP '2019-05-03 00:34:00',
    udf_round_timestamp_to_minute(TIMESTAMP '2019-05-03 00:34:59.999999', 2)
  )
