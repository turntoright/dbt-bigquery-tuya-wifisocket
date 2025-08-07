WITH hourly_usage AS(
  SELECT * FROM {{ref('v_6meldrum_usage_with_free_hours_hotwater')}}
  PIVOT(SUM(usage) FOR is_free IN('Yes' AS free_usage, 'No' AS usage))
)
, daily_usage AS(
  SELECT
    date,
    ROUND(SUM(free_usage),2) AS free_usage,
    ROUND(SUM(usage),2) AS usage
  FROM hourly_usage u
  GROUP BY 1
)
, daily_usage_rates AS (
  SELECT
    u.*,
    ROUND(u.free_usage*r.rate,4) AS savings,
    ROUND(u.usage*r.rate,4) AS usage_fee,
    r.daily_charge,
    ROUND(u.usage*r.rate+r.daily_charge,2) AS daily_fee
  FROM daily_usage u
  LEFT JOIN {{ref('rates')}} r
  ON u.date>=r.start_date AND u.date<=r.end_date  
)

SELECT * FROM daily_usage_rates
ORDER BY date