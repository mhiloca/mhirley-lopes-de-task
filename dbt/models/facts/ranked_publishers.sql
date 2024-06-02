SELECT
    "publisher",
    QUARTER(TO_DATE("created_date")) || '_' || YEAR(TO_DATE("created_date")) AS quarter_year,
    SUM(
        CASE
            WHEN "rank" = 1 THEN 5
            WHEN "rank" = 2 THEN 4
            WHEN "rank" = 3 THEN 3
            WHEN "rank" = 4 THEN 2
            WHEN "rank" = 5 THEN 1
        END
    ) AS total_points
FROM {{ source("NYT_BOOKS_RAW", "BOOKS") }}
GROUP BY 1, 2

