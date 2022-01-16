DROP TABLE edgar_cik_with_net_income_growth;
CREATE TABLE edgar_cik_with_net_income_growth AS (
    SELECT DISTINCT cik
    FROM edgar_net_income
    WHERE
        ratio >= 1.2
    GROUP BY cik
    HAVING COUNT(ratio) >= 3
    ORDER BY cik
);
