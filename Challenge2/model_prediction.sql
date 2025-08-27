SELECT
  *
FROM
  ML.PREDICT(MODEL `qwiklabs-gcp-04-db5d757ff012.Lab2.emergency_response_model`, (
    SELECT
      15.0 AS distance_to_station,
      4 AS units_available,
      'High' AS traffic_level,
      'Rainy' AS weather_condition,
      'Fire' AS call_type)
  );
