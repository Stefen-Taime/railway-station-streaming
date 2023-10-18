SELECT 
  id,
  incident_type,
  station,
  location,
  TIMESTAMP(timestamp) AS incident_timestamp,
  severity,
  duration
FROM 
  {{ source('staging_data', 'incidents') }}
