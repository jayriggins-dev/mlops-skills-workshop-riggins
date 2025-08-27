CREATE OR REPLACE TABLE
  `qwiklabs-gcp-04-db5d757ff012.lab1.Foo` AS
SELECT
  -- Retain original Applicant_ID and other unchanged fields
  Applicant_ID,
  Income,
  Number_of_Dependents,
  Amount_Requested,
  Application_Frequency_Last_Year,
  IP_Address,
  -- One-hot encode the 'Employment_Status' field
  CASE
    WHEN Employment_Status = 'Employed' THEN 1
    ELSE 0
  END AS employment_status_employed,
  CASE
    WHEN Employment_Status = 'Self-Employed' THEN 1
    ELSE 0
  END AS employment_status_self_employed,
  CASE
    WHEN Employment_Status = 'Unemployed' THEN 1
    ELSE 0
  END AS employment_status_unemployed,
  -- One-hot encode the 'Device_Type' field
  CASE
    WHEN Device_Type = 'Desktop' THEN 1
    ELSE 0
  END AS device_type_desktop,
  CASE
    WHEN Device_Type = 'Mobile' THEN 1
    ELSE 0
  END AS device_type_mobile,
  CASE
    WHEN Device_Type = 'Tablet' THEN 1
    ELSE 0
  END AS device_type_tablet,
  -- Create Age bins
  CASE
    WHEN Age BETWEEN 18 AND 24 THEN '18-24'
    WHEN Age BETWEEN 25 AND 34 THEN '25-34'
    WHEN Age BETWEEN 35 AND 44 THEN '35-44'
    WHEN Age BETWEEN 45 AND 54 THEN '45-54'
    WHEN Age BETWEEN 55 AND 64 THEN '55-64'
    WHEN Age >= 65 THEN '65+'
    ELSE '<18'
  END AS Age_Binned,
  -- Create Income-to-Amount-Requested ratio, handling division by zero
  SAFE_DIVIDE(Income, Amount_Requested) AS Income_to_Amount_Requested,
  -- Create Time_Since_Previous_Assistance in days
  -- Use COALESCE to handle NULL Previous_Assistance_Date
  DATE_DIFF(Application_Date, COALESCE(Previous_Assistance_Date, Application_Date), DAY) AS Time_Since_Previous_Assistance,
  -- Convert True/False to 1s and 0s
  CASE
    WHEN Previous_Assistance_Received = TRUE THEN 1
    ELSE 0
  END AS Previous_Assistance_Received,
  CASE
    WHEN Supporting_Doc_Verified = TRUE THEN 1
    ELSE 0
  END AS Supporting_Doc_Verified,
  -- Retain the 'Fraudulent' field
  Fraudulent
FROM
  `qwiklabs-gcp-04-db5d757ff012.lab1.Foo`;
