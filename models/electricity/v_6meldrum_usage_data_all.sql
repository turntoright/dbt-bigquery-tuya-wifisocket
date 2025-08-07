SELECT * FROM {{ref('usage_data_2024_12')}}
UNION ALL 
SELECT * FROM {{ref('usage_data_2025_01_06')}}
UNION ALL
SELECT * FROM {{ref('usage_data_2025_07')}}
UNION ALL
SELECT * FROM {{ref('usage_data_2025_08')}}
ORDER BY 1,2