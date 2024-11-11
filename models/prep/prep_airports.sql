WITH airports_reorder AS (
    SELECT 
        faa
        name,
        city,
        latt,
        lon,
        alt,
        tz,
        dst,
        city,
        country,
        region
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder