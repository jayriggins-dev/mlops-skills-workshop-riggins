CREATE OR REPLACE TABLE
  `qwiklabs-gcp-04-db5d757ff012.Lab3B.weather_keywords` AS (
    SELECT
      ml_generate_text_llm_result,
      city,
      state,
      weather_condition,
      temperature_f,
      wind_speed_mph
    FROM
      ML.GENERATE_TEXT(
        MODEL `qwiklabs-gcp-04-db5d757ff012.Lab3B.gemini_weather_model`,
        (
          SELECT
            city,
            state,
            weather_condition,
            temperature_f,
            wind_speed_mph,
            CONCAT(
              'For the following weather data, provide keywords. Answer in JSON format with one key: keywords. Keywords should be a list.',
              'City: ', city,
              ', State: ', state,
              ', Weather: ', weather_condition,
              ', Temperature: ', temperature_f, '°F',
              ', Wind: ', wind_speed_mph, 'mph.'
            ) AS prompt
          FROM
            `qwiklabs-gcp-04-db5d757ff012.Lab3B.weather_data_bravo`
          LIMIT 3
        ),
        STRUCT(
          0.2 AS temperature,
          TRUE AS flatten_json_output
        )
      )
);
