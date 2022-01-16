SELECT cik FROM edgar_gross_profit
WHERE cik NOT IN (
    SELECT cik FROM xbrl
    WHERE year IN (2015,2014,2013,2012,2011,2010)
)