SELECT 
  e.*
  , hw.dt_value AS hw_usage
  , ht.add_ele AS ht_usage
  , r.rate
FROM {{ref('v_6meldrum_usage_with_free_hours')}}  e
LEFT JOIN {{ref('v_6meldrum_usage_hotwater')}}  hw
ON e.date=hw.date AND e.hour=hw.hour
LEFT JOIN {{ref('v_6meldrum_usage_heater')}} ht
ON e.date=ht.date AND e.hour=ht.hour
LEFT JOIN {{ref('rates')}} r
ON e.date>=r.start_date AND e.date<=r.end_date
ORDER BY date,hour
