WITH quarterly_report AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY quarter_year ORDER BY total_points DESC) AS ranked_points,
        IFF(quarter_year = '1_2021', "publisher", NULL) AS q1_2021,
        IFF(quarter_year = '2_2021', "publisher", NULL) AS q2_2021,
        IFF(quarter_year = '3_2021', "publisher", NULL) AS q3_2021,
        IFF(quarter_year = '4_2021', "publisher", NULL) AS q4_2021,
        IFF(quarter_year = '1_2022', "publisher", NULL) AS q1_2022,
        IFF(quarter_year = '2_2022', "publisher", NULL) AS q2_2022,
        IFF(quarter_year = '3_2022', "publisher", NULL) AS q3_2022,
        IFF(quarter_year = '4_2022', "publisher", NULL) AS q4_2022,
        IFF(quarter_year = '1_2023', "publisher", NULL) AS q1_2023,
        IFF(quarter_year = '2_2023', "publisher", NULL) AS q2_2023,
        IFF(quarter_year = '3_2023', "publisher", NULL) AS q3_2023,
        IFF(quarter_year = '4_2023', "publisher", NULL) AS q4_2023
    FROM {{ ref("ranked_publishers") }}
    QUALIFY ranked_points <= 5
)

SELECT
    MIN(q2_2021) AS q2_2021,
    MIN(q3_2021) AS q3_2021,
    MIN(q4_2021) AS q4_2021,
    MIN(q1_2022) AS q1_2022,
    MIN(q2_2022) AS q2_2022,
    MIN(q3_2022) AS q3_2022,
    MIN(q4_2022) AS q4_2022,
    MIN(q1_2023) AS q1_2023,
    MIN(q2_2023) AS q2_2023,
    MIN(q3_2023) AS q3_2023,
    MIN(q4_2023) AS q4_2023
FROM quarterly_report
GROUP BY ranked_points
ORDER BY ranked_points
