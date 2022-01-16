DROP TABLE edgar_cik_with_gross_profit_growth;
CREATE TABLE edgar_cik_with_gross_profit_growth AS (
    SELECT DISTINCT cik
    FROM edgar_gross_profit
    WHERE
        ratio >= 1.2
    GROUP BY cik
    HAVING COUNT(ratio) >= 3
    ORDER BY cik
);
