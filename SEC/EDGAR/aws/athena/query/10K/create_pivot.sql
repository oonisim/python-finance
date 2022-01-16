CREATE TABLE edgar_gaap_pivot AS (
    SELECT
        g.cik, g.year, g.quarter, x."company name" as company_name,
        MAX(CASE g.rep  WHEN 'revenue' THEN CAST(g.value AS bigint) END) AS revenue,
        MAX(CASE g.name WHEN 'us-gaap:costofgoodsandservicessold' THEN CAST(g.value AS bigint) END) AS cost_of_revenue,
        MAX(CASE g.rep  WHEN 'gross_profit' THEN CAST(g.value AS bigint) END) AS gross_profit,
        MAX(CASE g.rep  WHEN 'operating_expense_sga' THEN CAST(g.value AS bigint) END) AS operating_expense_sga,
        MAX(CASE g.rep  WHEN 'operating_expense_rd' THEN CAST(g.value AS bigint) END) AS operating_expense_rd,
        MAX(CASE g.rep  WHEN 'net_income' THEN CAST(g.value AS bigint) END) AS net_income,
        MAX(CASE g.rep  WHEN 'cash_and_equivalent' THEN CAST(g.value AS bigint) END) AS cash_and_equivalent,
        MAX(CASE g.rep  WHEN 'current_assets' THEN CAST(g.value AS bigint) END) AS current_assets,
        MAX(CASE g.rep  WHEN 'total_assets' THEN CAST(g.value AS bigint) END) AS total_assets,
        MAX(CASE g.rep  WHEN 'current_liabilities' THEN CAST(g.value AS bigint) END) AS current_liabilities,
        MAX(CASE g.rep  WHEN 'total_liabilities' THEN CAST(g.value AS bigint) END) AS total_liabilities,
        MAX(CASE g.rep  WHEN 'stockholders_equity' THEN CAST(g.value AS bigint) END) AS stockholders_equity,
        MAX(CASE g.rep  WHEN 'shares_outstanding' THEN CAST(g.value AS bigint) END) AS shares_outstanding,
        MAX(CASE g.name WHEN 'us-gaap:earningspersharebasic' THEN ROUND(g.value, 2) END) AS eps
    FROM gaap g
    INNER JOIN xbrl as x ON
        g.cik = x.cik
        AND g.year = x.year
        AND g.quarter = x.quarter
    WHERE
        g."form type" IN ('10-K')
    GROUP BY g.cik, x."company name", g.year, g.quarter
    ORDER BY g.cik, x."company name", g.year ASC, g.quarter ASC
);