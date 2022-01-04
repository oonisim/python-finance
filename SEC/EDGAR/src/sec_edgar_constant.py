import os

# --------------------------------------------------------------------------------
# SEC
# --------------------------------------------------------------------------------
# SEC EDGAR
EDGAR_BASE_URL = "https://sec.gov/Archives"
EDGAR_HTTP_HEADERS = {"User-Agent": "Company Name myname@company.com"}
# SEC Form Types
SEC_FORM_TYPE_10K = "10-K"
SEC_FORM_TYPE_10Q = "10-Q"
SEC_FORM_TYPES = [SEC_FORM_TYPE_10Q, SEC_FORM_TYPE_10K]

# --------------------------------------------------------------------------------
# GAAP
# --------------------------------------------------------------------------------
FS_BS = "bs"                        # Financial Statement - Balance Sheet
FS_PL = "pl"                        # Financial Statement - Income Statement (PL)
FS_CF = "cf"                        # Financial Statement - Cash Flow Statement
FS_ITEM_TYPE_CREDIT = "credit"      # e.g Revenues
FS_ITEM_TYPE_DEBIT = "debit"        # e.g Long Term Debt
FS_ITEM_TYPE_METRIC = "metric"      # e.g EPS, P/E
FS_ITEM_TYPE_FACT = "fact"          # e.g Shared Outstandings

# Representatives
# Company can use different item, e.g. the final income for the period can be either:
# |us-gaap:profitloss   |usd|181847000|
# |us-gaap:netincomeloss|usd|180854000|
# To mark which one to use as the "final income", mark the row as "income".
FS_ITEM_REP_SHARES_OUTSTANDING = "shares_outstanding"
# PL
FS_ITEM_REP_REVENUE = "revenue"
FS_ITEM_REP_OP_COST = "operating_cost"                 # Operating Cost
FS_ITEM_REP_OP_INCOME = "operating_income"
FS_ITEM_REP_GROSS_PROFIT = "gross_profit"
FS_ITEM_REP_OPEX_RD = "operating_expense_rd"              # R/D Expeense
FS_ITEM_REP_OPEX_SGA = "operating_expense_sga"                 # Sales and Administrative Expense
FS_ITEM_REP_OPEX = "operating_expense"
FS_ITEM_REP_NET_INCOME = "net_income"
FS_ITEM_REP_EPS = "eps"
# BS
FS_ITEM_REP_CASH = "cash_and_equivalent"
FS_ITEM_REP_CURRENT_ASSETS = "current_assets"
FS_ITEM_REP_TOTAL_ASSETS = "total_assets"
FS_ITEM_REP_CURRENT_LIABILITIES = "current_liabilities"
FS_ITEM_REP_LIABILITIES = "total_liabilities"
FS_ITEM_REP_SE = "stockholders_equity"
FS_ITEM_REP_TOTAL_EQUITY = "total_equity"

# --------------------------------------------------------------------------------
# Platform
# --------------------------------------------------------------------------------
DEFAULT_LOG_LEVEL = 20  # INFO
NUM_CPUS = 8

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
DIR_DATA_XML_XBRL = os.path.realpath(f"{DIR_DATA}/xml/xbrl")
