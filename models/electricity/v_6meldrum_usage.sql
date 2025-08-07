SELECT * FROM {{source('electricity', 'gs_6meldrum_usage')}}
WHERE t00 IS NOT NULL
ORDER BY date