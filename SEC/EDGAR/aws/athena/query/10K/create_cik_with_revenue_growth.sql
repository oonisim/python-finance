DROP TABLE edgar_cik_with_revenue_growth;
CREATE TABLE edgar_cik_with_revenue_growth AS (
    SELECT DISTINCT cik
    FROM edgar_revenue
    WHERE
        ratio >= 1.2
    GROUP BY cik
    HAVING COUNT(ratio) >= 3
    ORDER BY cik
);

