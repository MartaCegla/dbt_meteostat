WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, DATE_PART('day', date) AS date_day
		, DATE_PART('month', date) AS date_month
		, DATE_PART('year',date) AS date_year
		, EXTRACT (week from date) AS cw
		, to_char (date,'FMMonth') AS month_name
		, to_char (date, 'day') AS weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN date_month in (12, 1, 2) THEN 'winter'
			WHEN date_month in (3, 4, 5) THEN 'spring'
            WHEN date_month in (6, 7, 8)  THEN 'summer'
            WHEN date_month in (9, 10, 11) THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date