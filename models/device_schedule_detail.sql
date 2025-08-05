with devices as (
    select * from {{ref('devices')}}
)
, device_schedule as (
    select * from {{ref('device_schedule')}}
)
, schedules as (
    select * from {{ref('schedules')}}
)
SELECT 
  d.*
  , ds.schedule_id
  , s.start_date
  , s.end_date
  , SPLIT(s.free_hours,'|') AS free_hours
FROM devices d
LEFT JOIN device_schedule ds
ON d.device_id=ds.device_id
LEFT JOIN schedules s
ON ds.schedule_id=s.id