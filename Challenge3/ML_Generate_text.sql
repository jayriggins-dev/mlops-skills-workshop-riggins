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
              'For the following weather data, provide keywords. Answer in text format with one key: keywords. Keywords should be a list.',
               'City: ', city,
              ', State: ', state,
              ', Temperature: ', temperature_f, '°F',
              ', Wind Speed: ', wind_speed_mph, 'mph',
              ', Precipitation: ', precipitation_in, ' in',
              ', Barometric Pressure: ', barometric_pressure_inHg, ' inHg',
              ', Humidity: ', humidity_percent, '%',
              ', Weather Condition: ', weather_condition, '.'
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
           
