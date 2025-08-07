WITH ele_log AS(
  SELECT 
    device_id,
    event_time,
    CAST((event_time - LAG(event_time) OVER (ORDER BY event_time))/1000 AS INT64) AS dt_time,
    value,
    value - LAG(value) OVER (ORDER BY event_time) AS dt_value,
    FORMAT_TIMESTAMP("%c",TIMESTAMP_MILLIS(event_time)) AS datetime_utc,
    FORMAT_TIMESTAMP("%c",TIMESTAMP_MILLIS(event_time),'Pacific/Auckland') AS datetime_nz,
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(event_time) AT TIME ZONE 'Pacific/Auckland') AS date,
    EXTRACT(HOUR FROM TIMESTAMP_MILLIS(event_time) AT TIME ZONE 'Pacific/Auckland') AS hour
  FROM {{source('tuya', 'tuya_ele_log_id')}}
  WHERE device_id='eb374a498ce4e60a19bsnm'
)
,tab_01 AS(
  SELECT  
    device_id,
    date,
    hour,
    DATETIME_ADD(DATETIME(date), INTERVAL hour HOUR) AS datetime,
    COUNT(*) AS data_count,
    MAX(value) AS value
  FROM ele_log
  GROUP BY 1,2,3
)
, final_01 AS (
  SELECT
    *,
    (value - LAG(value) OVER(ORDER BY date,hour))/1000 AS dt_value
  FROM tab_01
)

SELECT
  *
FROM final_01
ORDER BY datetime