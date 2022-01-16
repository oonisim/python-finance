DROP TABLE edgar_cik_with_gross_profit_slump;
CREATE TABLE edgar_cik_with_gross_profit_slump AS (
    SELECT DISTINCT cik
    FROM edgar_gross_profit
    WHERE
        ratio < 0.9
    GROUP BY cik
    HAVING COUNT(ratio) >= 3
    ORDER BY cik
);
