SELECT 
  event_time,
  lg.device_id,
  d.name AS device_name,
  add_ele/1000 AS add_ele,
  FORMAT_TIMESTAMP("%c",TIMESTAMP_MILLIS(event_time)) AS datetime_utc,
  FORMAT_TIMESTAMP("%c",TIMESTAMP_MILLIS(event_time),'Pacific/Auckland') AS datetime_nz,
  EXTRACT(DATE FROM TIMESTAMP_MILLIS(event_time) AT TIME ZONE 'Pacific/Auckland') AS date,
  EXTRACT(HOUR FROM TIMESTAMP_MILLIS(event_time) AT TIME ZONE 'Pacific/Auckland') AS hour
FROM {{ source('tuya', 'tuya_wifi_socket_add_ele_log')}} lg
LEFT JOIN {{ref('devices')}} d
ON lg.device_id=d.device_id