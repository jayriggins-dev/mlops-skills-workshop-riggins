CREATE OR REPLACE MODEL `qwiklabs-gcp-04-db5d757ff012.Lab3B.gemini_weather_model`
REMOTE WITH CONNECTION `projects/qwiklabs-gcp-04-db5d757ff012/locations/us/connections/riggins_vertex_connection`
OPTIONS (endpoint = 'gemini-2.0-flash')
