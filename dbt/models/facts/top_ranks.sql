WITH top_ranks AS (
    SELECT
        "list_id",
        "title",
        "author",
        "rank",
        "created_date"
    FROM {{ source("NYT_BOOKS_RAW", "BOOKS") }}
    WHERE
        "rank" IN (1, 2, 3)
        AND "created_date" BETWEEN '2022-01-01' AND '2022-12-31'
)

SELECT
    "title",
    "author",
    COUNT(DISTINCT "created_date") AS weeks_on_top_3
FROM top_ranks
GROUP BY 1, 2
ORDER BY weeks_on_top_3 DESC
