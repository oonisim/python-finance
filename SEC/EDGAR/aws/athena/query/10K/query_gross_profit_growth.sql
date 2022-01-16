WITH newbies AS (
    SELECT DISTINCT cik FROM xbrl
    WHERE year IN (2013,2012,2011,2010)
)
SELECT DISTINCT
    e.cik,
    x."company name",
    e.accession,
    e.year,
    e.quarter,
    e.gross_profit,
    e.increment,
    e.ratio
FROM
    edgar_gross_profit e
    INNER JOIN xbrl as x ON
        e.cik = x.cik
        AND e.year = x.year
        AND e.quarter = x.quarter
WHERE
    e.cik IN (SELECT cik from edgar_cik_with_gross_profit_growth)
    AND e.cik NOT IN (SELECT cik from edgar_cik_with_gross_profit_slump)
    /*AND e.cik NOT IN (SELECT cik FROM newbies)*/
ORDER BY
    e.cik, x."company name", e.year asc, e.quarter asc