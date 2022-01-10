import multiprocessing
import os

# --------------------------------------------------------------------------------
# SEC
# --------------------------------------------------------------------------------
# SEC
SEC_FORM_TYPE_10K = "10-K"
SEC_FORM_TYPE_10Q = "10-Q"
SEC_FORM_TYPES = [SEC_FORM_TYPE_10Q, SEC_FORM_TYPE_10K]
# SEC EDGAR
EDGAR_BASE_URL = "https://sec.gov/Archives"
EDGAR_HTTP_HEADERS = {"User-Agent": "Company Name myname@company.com"}

DF_COLUMN_CIK = 'CIK'
DF_COLUMN_COMPANY = 'Company Name'
DF_COLUMN_FORM_TYPE = 'Form Type'         # SEC Form Type, e.g. 10-K
DF_COLUMN_DATE_FILED = 'Date Filed'
DF_COLUMN_FILENAME = "Filename"
DF_COLUMN_FILEPATH = "Filepath"

DF_COLUMN_ACCESSION = "Accession"
DF_COLUMN_YEAR = "Year"
DF_COLUMN_QTR = "Quarter"
DF_COLUMN_FS = "FS"                       # Financial Statement, e.g. BS, PL
DF_COLUMN_REP = "Rep"
DF_COLUMN_TYPE = "Type"
DF_COLUMN_NAME = "Name"
DF_COLUMN_UNIT = "Unit"
DF_COLUMN_DECIMALS = "Decimals"
DF_COLUMN_CONTEXT = "Context"

# --------------------------------------------------------------------------------
# FS
# --------------------------------------------------------------------------------
FS_BS = "bs"                        # Financial Statement - Balance Sheet
FS_PL = "pl"                        # Financial Statement - Income Statement (PL)
FS_CF = "cf"                        # Financial Statement - Cash Flow Statement
FS_ELEMENT_TYPE_CREDIT = "credit"      # e.g Revenues
FS_ELEMENT_TYPE_DEBIT = "debit"        # e.g Long Term Debt
FS_ELEMENT_TYPE_METRIC = "metric"      # e.g EPS, P/E
FS_ELEMENT_TYPE_FACT = "fact"          # e.g Shared Outstanding
FS_ELEMENT_TYPE_CALC = 'calc'          # calculated value e.g. sub total

# Representatives
# Company can use different item, e.g. the final income for the period can be either:
# |us-gaap:profitloss   |usd|181847000|
# |us-gaap:netincomeloss|usd|180854000|
# To mark which one to use as the "final income", mark the row as "income".
FS_ELEMENT_REP_SHARES_OUTSTANDING = "shares_outstanding"
# PL
FS_ELEMENT_REP_REVENUE = "revenue"
FS_ELEMENT_REP_OP_COST = "operating_cost"          # Operating Cost
FS_ELEMENT_REP_OP_INCOME = "operating_income"
FS_ELEMENT_REP_GROSS_PROFIT = "gross_profit"
FS_ELEMENT_REP_OPEX_RD = "operating_expense_rd"    # R/D Expeense
FS_ELEMENT_REP_OPEX_SGA = "operating_expense_sga"  # Sales and Administrative Expense
FS_ELEMENT_REP_OPEX = "operating_expense"
FS_ELEMENT_REP_NET_INCOME = "net_income"
FS_ELEMENT_REP_DIVIDEND = "dividend"
FS_ELEMENT_REP_TAX = "tax"
FS_ELEMENT_REP_EPS = "eps"
# BS
FS_ELEMENT_REP_CASH = "cash_and_equivalent"
FS_ELEMENT_REP_CURRENT_ASSETS = "current_assets"
FS_ELEMENT_REP_TOTAL_ASSETS = "total_assets"
FS_ELEMENT_REP_CURRENT_LIABILITIES = "current_liabilities"
FS_ELEMENT_REP_LIABILITIES = "total_liabilities"
FS_ELEMENT_REP_EQUITY = "stockholders_equity"       # Total Equity
FS_ELEMENT_REP_EQUITY_AND_LIABILITIES = "total_equity_and_liabilities"

# --------------------------------------------------------------------------------
# Platform
# --------------------------------------------------------------------------------
DEFAULT_LOG_LEVEL = 20  # INFO
NUM_CPUS = multiprocessing.cpu_count()
MAX_NUM_WORKERS = NUM_CPUS * 4

# --------------------------------------------------------------------------------
# Directories
# Name represents the directory structure, e.g. DIR_DATA_CSV_INDEX corresponds to
# "${DIR}/data/csv/index" where DIR_DATA is the root directory for data
# --------------------------------------------------------------------------------
DIR = os.path.dirname(os.path.realpath(__file__))
# Data
DIR_DATA = os.path.realpath(f"{DIR}/../data/")
DIR_DATA_CSV_INDEX = os.path.realpath(f"{DIR_DATA}/csv/index")
DIR_DATA_CSV_LIST = os.path.realpath(f"{DIR_DATA}/csv/listing")
DIR_DATA_CSV_XBRL = os.path.realpath(f"{DIR_DATA}/csv/xbrl")
DIR_DATA_CSV_GAAP = os.path.realpath(f"{DIR_DATA}/csv/gaap")
DIR_DATA_XML_XBRL = os.path.realpath(f"{DIR_DATA}/xml/xbrl")

"""
# OPTICAL CABLE CORPORATION 10-K for the fiscal year ended October 31, 2021
CIK = '0001000230'
ACCESSION = '000143774921028951'
FORM_TYPE = SEC_FORM_TYPE_10K


# AMKOR 2020 10K
CIK = '1047127'
ACCESSION = '0001047127-20-000006'.replace('-', '')
FORM_TYPE = SEC_FORM_TYPE_10K


# DIOD 2020 10K
CIK = '29002'
ACCESSION = '000156459021007008'
FORM_TYPE = SEC_FORM_TYPE_10K


# AMKOR 2021 10Q
CIK = '1047127'
ACCESSION = '000104712721000043'
FORM_TYPE = SEC_FORM_TYPE_10Q


# QORVO 2021 10K
CIK = '1604778'
ACCESSION = '000160477821000032'
FORM_TYPE = SEC_FORM_TYPE_10K


INDEX_XML_URL = f"https://www.sec.gov/Archives/edgar/data/{CIK}/{ACCESSION}/index.xml"
"""
