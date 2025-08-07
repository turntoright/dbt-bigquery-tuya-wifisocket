SELECT 
  b.*,
  COUNT(u.date) AS days,
  ROUND(SUM(u.free_usage),2) AS u_free_usage,
  ROUND(SUM(u.usage)+SUM(u.free_usage),2) AS u_total_usage,
  ROUND(SUM(u.daily_charge),2) AS u_daily_charge,
  ROUND(SUM(u.daily_fee),2) AS u_total_cost
FROM {{ref('bills_meridian')}} b
LEFT JOIN {{ref('v_6meldrum_daily_usage_rate')}} u
ON u.date>=b.start_date AND u.date<=b.end_date
GROUP BY 1,2,3,4,5
ORDER BY start_date