CREATE OR REPLACE MODEL
  `qwiklabs-gcp-04-db5d757ff012.Lab2.emergency_response_model`
OPTIONS (
  model_type = 'LINEAR_REG',
  input_label_cols = ['response_time']
) AS
SELECT
  distance_to_station,
  units_available,
  traffic_level,
  weather_condition,
  call_type,
  response_time
FROM
  `qwiklabs-gcp-04-db5d757ff012.Lab2.emergency_calls_response_time`;
