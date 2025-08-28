
# Commented out IPython magic to ensure Python compatibility.
# #create dataset
# %%bigquery
# CREATE SCHEMA IF NOT EXISTS weather_data_dataset
# OPTIONS(
#  location="us");
# 
# #create Table
# CREATE OR REPLACE EXTERNAL TABLE `qwiklabs-gcp-04-db5d757ff012.Lab3B.weather_data_bravo`
# OPTIONS (
#   format = 'CSV',
#   uris = ['gs://labs.roitraining.com/data-to-ai-workshop/weather_data.csv'],
#   skip_leading_rows = 1,
#   allow_jagged_rows = false,
#   allow_quoted_newlines = false,
#   field_delimiter = ',',
#   max_bad_records = 10
# )

# Commented out IPython magic to ensure Python compatibility.
# #Create Connection in Bigqurry to Vertex AI
# %%bigquery
# CREATE OR REPLACE MODEL
#   `qwiklabs-gcp-04-db5d757ff012.Lab3B.gemini_weather_model` REMOTE
# WITH CONNECTION DEFAULT OPTIONS (ENDPOINT = 'gemini-2.0-flash');
#

# Commented out IPython magic to ensure Python compatibility.
# %%bigquery
# CREATE OR REPLACE TABLE
# `qwiklabs-gcp-04-db5d757ff012.Lab3B.weather_keywords` AS (
# SELECT *
# FROM
# ML.GENERATE_TEXT(
# MODEL `qwiklabs-gcp-04-db5d757ff012.Lab3B.gemini_weather_model`,
# (
#    SELECT CONCAT(
#        'Provide a narrative weather forcast for the following data: ',
#        date, city, state, temperature_f, wind_speed_mph, precipitation_in, barometric_pressure_inHg, humidity_percent, weather_condition
#    ) AS prompt
#    FROM `qwiklabs-gcp-04-db5d757ff012.Lab3B.weather_data_bravo`
# ),
# STRUCT(
#    0.2 AS temperature, TRUE AS flatten_json_output)));
           
