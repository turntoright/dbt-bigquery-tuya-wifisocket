SELECT 
    date,
    CAST(SUBSTR(time_period,0,2) AS INT64) AS hour,
    is_free,
    SUM(usage) AS usage
FROM {{ref('v_6meldrum_usage_data_all')}}
GROUP BY 1,2,3