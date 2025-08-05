SELECT 
  event_time,
  device_id,
  add_ele/1000 AS add_ele,
  FORMAT_TIMESTAMP("%c",TIMESTAMP_MILLIS(event_time)) AS datetime_utc,
  FORMAT_TIMESTAMP("%c",TIMESTAMP_MILLIS(event_time),'Pacific/Auckland') AS datetime_nz,
  EXTRACT(DATE FROM TIMESTAMP_MILLIS(event_time) AT TIME ZONE 'Pacific/Auckland') AS date,
  EXTRACT(HOUR FROM TIMESTAMP_MILLIS(event_time) AT TIME ZONE 'Pacific/Auckland') AS hour
FROM {{ source('tuya', 'tuya_wifi_socket_add_ele_log')}}