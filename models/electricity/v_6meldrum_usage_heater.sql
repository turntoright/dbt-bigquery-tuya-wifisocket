SELECT 
    date,
    hour,
    is_free,
    SUM(add_ele) AS add_ele
FROM {{ref('device_consumption')}} dc
LEFT JOIN {{ref('devices')}} d
ON dc.device_id=d.device_id
WHERE d.usage_type='Heater'
GROUP BY 1,2,3