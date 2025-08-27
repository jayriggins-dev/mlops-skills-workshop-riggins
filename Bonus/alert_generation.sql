CREATE OR REPLACE TABLE
  `qwiklabs-gcp-04-db5d757ff012.Bonus.weather_alerts` AS (
    SELECT
      ml_generate_text_result['predictions'][0]['content'] AS generated_alert,
      t.*
    FROM
      ML.GENERATE_TEXT(
        MODEL `qwiklabs-gcp-04-db5d757ff012.Lab3B.gemini_weather_model`,
        (
          SELECT
            -- The prompt instructs Gemini on how to create the alert.
            'Create a professional and concise weather alert. The alert should include the forecast for the day. Here is the data: '
            || 'Airport: ' || t.name
            || ', City: ' || t.municipality
            || ', State: ' || t.iso_region
            || ', Weather: ' || t.weather_condition
            || ', Temperature: ' || t.temperature
            || '°F, Wind: ' || t.wind_speed || ' mph.' AS prompt,
            t.*
          FROM
            `qwiklabs-gcp-04-db5d757ff012.Bonus.weather_forecasts` AS t
          LIMIT 10
        ),
        STRUCT(0.2 AS temperature)
      )
  );
