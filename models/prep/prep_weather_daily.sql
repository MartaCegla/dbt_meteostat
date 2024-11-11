WITH daily_data AS (
        SELECT * 
        FROM {{ref('staging_weather_daily')}}
    ),
    add_features AS (
        SELECT *
    		, DATE_PART('day', date) AS date_day
    		, DATE_PART('month', date) AS date_month
    		, DATE_PART('year', date) AS date_year
    		, EXTRACT(WEEK from date) AS cw -- Adjusted for general SQL compatibility
    		, TO_CHAR(date, 'Month') AS month_name
    		, TO_CHAR(date, 'Day') AS weekday
        FROM daily_data 
    ),
    add_more_features AS (
        SELECT *
    		, (CASE 
    			WHEN date_part('month', date) in (12, 1, 2) THEN 'winter'
    			WHEN date_part('month', date) in (3, 4, 5) THEN 'spring'
                WHEN date_part('month', date) in (6, 7, 8) THEN 'summer'
                WHEN date_part('month', date) in (9, 10, 11) THEN 'autumn'
    		END) AS season
        FROM add_features
    )
    SELECT *
    FROM add_more_features
    ORDER BY date