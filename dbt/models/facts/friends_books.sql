SELECT
    "title",
    "author",
    IFF("rank" = 1, 'jake', 'pete') AS team
FROM {{ source("NYT_BOOKS_RAW", "BOOKS") }}
WHERE "rank" IN (1, 3)
QUALIFY ROW_NUMBER() OVER (PARTITION BY "title" ORDER BY "created_date") = 1
ORDER BY "title"

