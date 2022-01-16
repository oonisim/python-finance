DROP TABLE edgar_cik_with_net_income_slump;
CREATE TABLE edgar_cik_with_net_income_slump AS (
    SELECT DISTINCT cik
    FROM edgar_net_income
    WHERE
        ratio < 0.9
    GROUP BY cik
    HAVING COUNT(ratio) >= 3
    ORDER BY cik
);
