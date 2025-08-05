SELECT
    lg.device_id
    , ds.name AS device_name
    , lg.date
    , lg.hour
    , CAST(lg.hour AS STRING) IN UNNEST(ds.free_hours) AS is_free
    , SUM(lg.add_ele) AS add_ele
FROM {{ref('view_tuya_wifi_socket_add_ele_log')}} lg
LEFT JOIN {{ref('device_schedule_detail')}} ds
ON lg.device_id=ds.device_id AND (lg.date BETWEEN ds.start_date AND ds.end_date)
GROUP BY 1,2,3,4,5