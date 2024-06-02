WITH unique_lists AS (
    SELECT
        "list_id",
        "book_uri",
        "created_date"
    FROM {{ source("NYT_BOOKS_RAW", "BOOKS") }}
)

SELECT ul."list_id", l."list_name", COUNT(DISTINCT ul."book_uri") AS num_unique_books
FROM unique_lists ul
JOIN {{ source("NYT_BOOKS_RAW", "LISTS") }} l
    ON ul."list_id" = l."list_id"
GROUP BY 1, 2
ORDER BY num_unique_books
LIMIT 3