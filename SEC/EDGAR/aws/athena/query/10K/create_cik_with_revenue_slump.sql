DROP TABLE edgar_cik_with_revenue_slump;
CREATE TABLE edgar_cik_with_revenue_slump AS (
    SELECT DISTINCT cik
    FROM edgar_revenue
    WHERE
        ratio < 0.9
    GROUP BY cik
    HAVING COUNT(ratio) >= 3
    ORDER BY cik
);
