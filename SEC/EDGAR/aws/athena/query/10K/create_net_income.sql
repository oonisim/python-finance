CREATE TABLE edgar_net_income AS (
    WITH X AS (
        SELECT
            cik,
            accession,
            year,
            quarter,
            "form type" as form_type,
            value
        FROM gaap
        WHERE
            "form type" IN ('10-K')
            AND rep = 'net_income'
        ORDER BY
            cik, year ASC, quarter ASC
    )
    SELECT DISTINCT
    	cik,
    	accession,
    	year,
    	quarter,
    	form_type,
    	CAST(value AS bigint) as net_income,
    	CAST(value - lag(value) over (partition by cik) AS bigint) as increment,
    	ROUND(value / lag(value) over (partition by cik),2) as ratio
    FROM X
)
