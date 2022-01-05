"""
#!/usr/bin/env python
# coding: utf-8

# Parsing SEC Filing XBRL Document


## Objective

Parse the filing XBRL file to create a DOM like structure that represent the filing data

## References

* [XBRL Specification - Extensible Business Reporting Language (XBRL) 2.1](https://www.xbrl.org/Specification/XBRL-2.1/REC-2003-12-31/XBRL-2.1-REC-2003-12-31+corrected-errata-2013-02-20.html)

* [List of US GAAP Standards](https://xbrlsite.azurewebsites.net/2019/Prototype/references/us-gaap/)
* [XBRL US - List of Elements](https://xbrl.us/data-rule/dqc_0015-le/)

**Element Version**|**Element ID**|**Namespace**|**Element Label**|**Element Name**|**Balance Type**|**Definition**
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:
1|1367|us-gaap|Interest Expense|InterestExpense|debit|Amount of the cost of borrowed funds accounted for as interest expense.
2|2692|us-gaap|Cash and Cash Equivalents, at Carrying Value|CashAndCashEquivalentsAtCarryingValue|debit|Amount of currency on hand as well as demand deposits with banks or financial institutions. Includes other kinds of accounts that have the general characteristics of demand deposits. Also includes short-term, highly liquid investments that are both readily convertible to known amounts of cash and so near their maturity that they present insignificant risk of changes in value because of changes in interest rates. Excludes cash and cash equivalents within disposal group and discontinued operation.

## XBRL Element

* [XBRL Glossary of Terms](https://www.sec.gov/page/osd_xbrlglossary)
* [XBRL - What is us-gaap:OperatingSegmentsMember element anb where is it defined?](https://money.stackexchange.com/questions/148010/xbrl-what-is-us-gaapoperatingsegmentsmember-element-anb-where-is-it-defined)

### Example
For instance, Qorvo 2020 10K

* [XBRL/rfmd-20210403_htm.xml](https://www.sec.gov/Archives/edgar/data/1604778/000160477821000032/rfmd-20210403_htm.xml)
* [HTML/rfmd-20210403.htm)](https://www.sec.gov/Archives/edgar/data/1604778/000160477821000032/rfmd-20210403.htm):

```
<us-gaap:cashandcashequivalentsatcarryingvalue contextref="*" decimals="-3" id="..." unitref="usd">
  1397880000
</us-gaap:cashandcashequivalentsatcarryingvalue>,
<us-gaap:cashandcashequivalentsatcarryingvalue contextref="***" decimals="-3" id="..." unitref="usd">
  714939000
</us-gaap:cashandcashequivalentsatcarryingvalue>,
<us-gaap:cashandcashequivalentsatcarryingvalue contextref="***" decimals="-3" id="..." unitref="usd">
 711035000
</us-gaap:cashandcashequivalentsatcarryingvalue>
```
"""
import datetime
import logging
import re
from typing import (
    List
)

import bs4
import dateutil
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

# regexp to extract numeric string
REGEXP_NUMERIC = re.compile(r"\s*[\d.-]*\s*")


# ## Company Name
def get_company_name(soup):
    """Get company (registration) name from the XBRL"""
    registrant_name = soup.find("dei:EntityRegistrantName".lower()).string.strip()
    assert registrant_name, f"No registrant name found"

    # Remove non-ascii characters to form a company name
    registrant_name = ''.join(e for e in registrant_name if e.isalnum())
    logging.debug("get_company_name(): company name is [%s]" % registrant_name)
    return registrant_name

# --------------------------------------------------------------------------------
# # Repoting period
#
# Each 10-K and 10-Q XBRL has the reporting period for the filing. To exclude the other period, e.g. pervious year or quarter, use the ```context id``` for the reporting period. **Most** 10-K, 10-Q specify the annual period with the first ```<startDate> and <endDate>``` tags.
# For instances:
#
# ### QRVO 10-K 2020
# ```
# <context id="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403">
#     <entity>
#     <identifier scheme="http://www.sec.gov/CIK">0001604778</identifier>
#     </entity>
#     <period>
#         <startDate>2020-03-29</startDate>
#         <endDate>2021-04-03</endDate>
#     </period>
# </context>
# ```
#
# ### AMKR 10-K 2020
# ```
# <context id="i5fac0a392353427b8266f185495754d3_D20200101-20201231">
#     <entity>
#     <identifier scheme="http://www.sec.gov/CIK">0001047127</identifier>
#     </entity>
#     <period>
#         <startDate>2020-01-01</startDate>
#         <endDate>2020-12-31</endDate>
#     </period>
# </context>
# ```
#
# ### AAPL 10-Q 4th QTR 2020
# ```
# <context id="i6e431846933d461fb8c8c0bdf98c9758_D20200927-20201226">
#     <entity>
#     <identifier scheme="http://www.sec.gov/CIK">0000320193</identifier>
#     </entity>
#     <period>
#         <startDate>2020-09-27</startDate>
#         <endDate>2020-12-26</endDate>
#     </period>
# </context>
# ```
#
# **However, there are companies that do not have this manner**.
# For instance [10-K for OPTICAL CABLE CORPORATION(CIK=0001000230)]
# (https://www.sec.gov/Archives/edgar/data/1000230/000143774921028951/occ20211031_10k_htm.xml)
# has the same start and end dates at first.
# ```
# <context id="d202110K">
#     <entity>
#         <identifier scheme="http://www.sec.gov/CIK">0001000230</identifier>
#     </entity>
#     <period>
#         <startDate>2021-10-31</startDate>   # <-----
#         <endDate>2021-10-31</endDate>
#     </period>
# </context>
# <context id="d_2020-11-01_2021-10-31">
#     <entity>
#         <identifier scheme="http://www.sec.gov/CIK">0001000230</identifier>
#     </entity>
#     <period>
#         <startDate>2020-11-01</startDate>   # <-----
#         <endDate>2021-10-31</endDate>
#     </period>
# </context>
# ```
#
# The report uses the 2nd for 2021 F/S element value but does not use the first one.
#
# **B/S**
# ```
# <us-gaap:Assets
#     contextRef="i_2021-10-31"    # <-----
#     decimals="INF"
#     id="c79893606"
#     unitRef="USD"
# >
#   37916530
# </us-gaap:Assets>
# ```
#
# **P/L**
# ```
# <us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax
#   contextRef="d_2020-11-01_2021-10-31"     # <-----
#   decimals="INF"
#   id="c79893662"
#   unitRef="USD"
# >
# 59136294
# </us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax>
# ```
#
# <img src='../image/edgar_optical_cable_2021_10K.png' align="left" width=500/>
# ### Get the period from the 1st
#
# For now, just get the period from the 1st **period** element.
# --------------------------------------------------------------------------------
def report_period_end_date(soup):
    """Identify the end date of the report period from the first "context" tag
    in the XBRL that has <period><startDate> tag as its child tag. 

    <context id="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403">
        <entity>
        <identifier scheme="http://www.sec.gov/CIK">0001604778</identifier>
        </entity>
        <period>
            <startDate>2020-03-29</startDate>
            <endDate>2021-04-03</endDate>        # <-----
        </period>
    </context>

    Args:
        soup: Source BS4
    Returns: reporting period e.g. "2021-09-30"
    """
    first_context = None
    for context in soup.find_all('context'):
        if context.find('period') and context.find('period').find('enddate'):
            first_context = context
            break      
        
    assert first_context is not None, "No period found"
    period = first_context.find('period').find('enddate').text.strip()
    
    try:
        dateutil.parser.parse(period)
    except ValueError as e:
        assert False, f"Invalid period {period} found."
            
    return period



# ### Regexp to find all the contexts that match with report_period_end_date
# 
# 10-K, 10-Q F/S uses multiple contexts to refer to the F/S element values for the **period**. Collect all the contexts for the **period**.

# In[21]:


def target_context_ids(soup, period_end_date: str, form_type: str):
    """
    Extract all the contexts that refer to the reporting period of the filing.

    XBRL (10-K,10-Q) uses multiple contexts to refer to the F/S items in the
    reporting period. Collect all the contexts for the period.
    
    [Example]
    Context ID="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403" for the
    10-K reporting period (fiscal year ended April 3, 2021).
    
    <context id="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403">
        <entity>
        <identifier scheme="http://www.sec.gov/CIK">0001604778</identifier>
        </entity>
        <period>
            <startDate>2020-03-29</startDate>
            <endDate>2021-04-03</endDate>         # <--- end of the period
        </period>
    </context>
    
    Revenues for Fiscal 2021 has the context ID of the period.
    <us-gaap:Revenues
        contextRef="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403" <---
        decimals="-3" 
        id=...
        unitRef="usd"
    >
    4015307000
    </us-gaap:Revenues>
    
    Revenues for Fiscal 2020 has a different context ID.    
    <us-gaap:Revenues 
        contextRef="i8cb18639eb7241dc9a6d20dd6ef765f3_D20190331-20200328" 
        decimals="-3" 
        id=...
        unitRef="usd"
    >
    3239141000
    </us-gaap:Revenues>
    
    Args:
        soup: BS4 source
        period_end_date: Dnd date of the reporting period
        form_type: Filing form type e.g. FORM_TYPE_10K
    """
    end_date = dateutil.parser.parse(period_end_date)
    if form_type == SEC_FORM_TYPE_10K:
        duration = datetime.timedelta(days=365) 
    elif form_type == SEC_FORM_TYPE_10Q:
        datetime.timedelta(days=90)
    else:
        assert f"The form type {form_type} is not currently supported"

    # --------------------------------------------------------------------------------
    # (from, to) range wherein the start date of the reprting period should exits.
    #
    # It is expected that report_start_date = (report_end_date - duration) +/- 30 days.
    # If 10-K period ends 2021-04-03, the start date should be in-between the two:
    # 2020-03-04 (end_date - duration - datetime.timedelta(days=30) and 
    # 2020-05-03 (end_date - duration + datetime.timedelta(days=30).
    # --------------------------------------------------------------------------------
    from_date = end_date - duration - datetime.timedelta(days=30)
    to_date = end_date - duration + datetime.timedelta(days=30)

    # --------------------------------------------------------------------------------
    # Contexts with 'startdate' child tag
    # Find the context tags whose <period><startDate> child tag value is in-between
    # (from, to) range. Those are the tags that refer to the current reporting period.
    # --------------------------------------------------------------------------------
    target_context_ids = []
    for context in soup.find_all('context'):
        # Find context whose <period>/<endDate> child tag value matches the end date of the period
        if context.find('period') and context.find('period').find('enddate', string=period_end_date):
            # Get the <period><startDate> value
            start = dateutil.parser.parse(context.find('period').find('startdate').text)
            # --------------------------------------------------------------------------------
            # If the startDate is in-between (from, to), or equal to period_end_date,
            # then the context is the one that refers to the current report period.
            #
            # Why "start date equal to period end date"?
            # [<period><startDate> == <period><endDate>] can happen in the "context" tag
            # that refers to the current reporting period for some companies e.g. 
            # 10-K of October 31, 2021 by Optical Cable Corporation.
            # --------------------------------------------------------------------------------
            if start == end_date or (from_date < start and start < to_date):
                target_context_ids.append(context['id'])

    # --------------------------------------------------------------------------------
    # Contexts with 'instant' child tag
    # Find the context tags whose <period><instant> has the period_end_date value.
    # Those are the tags that also refer to the current reporting period.
    # --------------------------------------------------------------------------------
    target_context_ids.extend([
        context['id'] for context in soup.find_all('context') 
        if context.find('period') and context.find('period').find(['instant'], string=period_end_date)
    ])
    
    return target_context_ids


def get_regexp_to_match_xbrl_contexts(report_period_end_date: str, form_type: str) -> re.Pattern:
    return re.compile("|".join(
        target_context_ids(soup=soup, period_end_date=report_period_end_date, form_type=form_type))
    )

def assert_bf4_tag(element):
    assert isinstance(element, bs4.element.Tag),     f"Expected BS4 tag but {element} of type {type(element)}"

def display_elements(elements):
    assert isinstance(elements, bs4.element.ResultSet) or isinstance(elements[0], bs4.element.Tag)
    for element in elements: # decimals="-3" means the displayed value is divied by 1000.
        print(f"{element.name:80} {element['unitref']:5} {element['decimals']:5} {element.text:15}")

def get_element_hash(element):
    """Generate the financial element hash key to uniquely identify an financial element record
    In a F/S, the same element, e.g. gaap:CashAndCashEquivalentsAtCarryingValue can be used at
    multiple places, one in B/S and one in P/L.
    
    To be able to identify if two elements are the same, provides a way to be able to compare
    two elements by generating a hash key from the attributes of an element.
    
    Args:
        element: bs4.element.Tag for an financial element
    Returns: hash key
    """
    assert isinstance(element, bs4.element.Tag)
    # key = f"{element.name}{element['unitref']}{element['contextref']}{element.text}"
    # key = f"{element.name}{element['unitref']}{element.text}"
    key = f"{element.name}{element['unitref']}"
    return hash(key)


# XBRL attribute conditions to match when extracting FS elements
ATTRIBUTES = {
    "contextref": get_regexp_to_match_xbrl_contexts(TBD, TBD),
    "decimals": True, 
    "unitref": True
}


def find_financial_elements(soup, element_names, attributes=ATTRIBUTES):
    """Find the financial statement elements from the XML/HTML source.
    Args:
        soup: BS4 source
        element_names: String or regexp instance to select the financial elements.
        attribute: tag attributes to select the financial elements
    Returns:
        List of BS4 tag objects that matched the element_names and attributes.
    """
    assert isinstance(soup, BeautifulSoup)
    assert isinstance(element_names, re.Pattern) or isinstance(element_names, str)

    
    names = element_names.lower() if isinstance(element_names, str) else element_names

    elements = soup.find_all(
        name=names,
        string=REGEXP_NUMERIC,
        attrs=attributes
    )
    
    # Select unique elements
    hashes = set([]) 
    results = []
    if elements is not None and len(elements) > 0:
        for element in elements:
            hash_value = get_element_hash(element)
            if hash_value not in hashes:
                results.append(element) 
                hashes.add(hash_value)

    return results


for element in find_financial_elements(soup, re.compile(f"^{NAMESPACE_GAAP}:.*"), attributes=ATTRIBUTES):
    print(f"{element.name:100}:{element.text}")


def get_financial_element_numeric_values(elements):
    # assert isinstance(elements, bs4.element.ResultSet) or isinstance(elements[0], bs4.element.Tag)
    assert_bf4_tag(elements[0])
    
    values = []
    for element in elements:
        assert re.match(REGEXP_NUMERIC, element.text.strip()), f"Element must be numeric but {element.text}"
        values.append(float(element.text))
        
    return values


def get_financial_element_columns():
    """Financial record columns"""
    return [
        "FS",           # Which financial statement e.g. bs for Balance Sheet
        "Rep",          # Representative marker e.g. "income" or "cogs" (cost of goods sold)
        "Type",         # "debit" or "credit"
        "Name",         # F/S item name, e.g. us-gaap:revenues
        "Value",
        "Unit",         # e.g. USD
        "Decimals",     # Scale
        "Context"       # XBRL context ID
    ]

def get_record_for_nil_elements(elements, element_type=None):
    results = []
    for element in elements:
        # F/S
        element_fs = ""
        
        # Rep
        element_rep = ""
        
        # Type of the elemen
        if element_type is None and element.name in GAAP_DEBIT_ITEMS: element_type = FS_ELEMENT_TYPE_DEBIT
        if element_type is None and element.name in GAAP_CREDIT_ITEMS: element_type = FS_ELEMENT_TYPE_CREDIT
        if element_type is None and element.name in GAAP_CALC_ITEMS: element_type = FS_ELEMENT_TYPE_CALC
        if element_type is None and element.name in GAAP_METRIC_ITEMS: element_type = FS_ELEMENT_TYPE_METRIC
        if element_type is None and element.name in GAAP_FACT_ITEMS: element_type = FS_ELEMENT_TYPE_FACT

        # Name of the financial element
        element_name = element.name

        # Unit of the financial element
        element_unit = element['unitref']

        # Scale of the element
        element_scale = int(element['decimals']) if element['decimals'].lower() != 'inf' else np.inf

        # Value of the element
        element_value = -np.inf

        # Context ID of the element
        element_context = element['contextref']

        record = [
            element_fs,
            element_rep,
            element_type,
            element_name,
            element_value, 
            element_unit, 
            element_scale, 
            element_context
        ]
        assert len(record) == len(get_financial_element_columns())
        results.append(record)
        
    return results

def get_records_for_financial_elements(elements, element_type=None):
    """Financial record having the columns of get_financial_element_columns"""
    # assert isinstance(elements, bs4.element.ResultSet) or isinstance(elements[0], bs4.element.Tag)
    assert_bf4_tag(elements[0])
    
    results = []
    for element in elements:
        # F/S
        element_fs = ""
        
        # Rep
        element_rep = ""
        
        # Type of the elemen
        if element_type is None and element.name in GAAP_DEBIT_ITEMS: element_type = ELEMENT_TYPE_DEBIT
        if element_type is None and element.name in GAAP_CREDIT_ITEMS: element_type = ELEMENT_TYPE_CREDIT
        if element_type is None and element.name in GAAP_CALC_ITEMS: element_type = ELEMENT_TYPE_CALC
        if element_type is None and element.name in GAAP_METRIC_ITEMS: element_type = ELEMENT_TYPE_METRIC
        if element_type is None and element.name in GAAP_FACT_ITEMS: element_type = ELEMENT_TYPE_FACT
        
        # Name of the financial element
        element_name = element.name
        
        # Unit of the financial element
        element_unit = element['unitref']
        
        # Scale of the element
        element_scale = int(element['decimals']) if element['decimals'].lower() != 'inf' else np.inf
            
        # Value of the element
        element_value = float(element.text)

        # Context ID of the element
        element_context = element['contextref']

        record = [
            element_fs,
            element_rep,
            element_type,
            element_name,
            element_value, 
            element_unit, 
            element_scale, 
            element_context
        ]
        assert len(record) == len(get_financial_element_columns())
        results.append(record)
        
    return results


def represents(records: list, fs: str, rep: str):
    assert isinstance(records, list) and len(fs) > 0 and len(rep) > 0
    
    row = records[0]
    row[0] = fs
    row[1] = rep
    
    return records


# In[33]:


def get_records_for_financial_element_names(soup, names: List[str], element_type=None):
    """Get financial records that matches the financial element names
    """
    elements = find_financial_elements(soup=soup, element_names=names)
    if len(elements) > 0:
        display_elements(elements)
        return get_records_for_financial_elements(elements, element_type)
    else:
        return get_record_for_nil_elements(elements)


# In[34]:


def get_values_for_financial_element_names(soup, names: List[str]):
    elements = find_financial_elements(soup=soup, element_names=names)
    if len(elements) > 0:
        display_elements(elements)
        return get_financial_element_numeric_values(elements)
    else:
        return []


# ---
# # Shares Outstanding
def get_shares_outstanding(soup):
    names = re.compile("|".join([
        rf"{NAMESPACE_GAAP}:SharesOutstanding",
        rf"{NAMESPACE_GAAP}:CommonStockSharesOutstanding",
        rf"{NAMESPACE_GAAP}:CommonStockOtherSharesOutstanding",
    ]).lower())

    return get_records_for_financial_element_names(soup=soup, names=names)


shares_outstandings = get_shares_outstanding(soup)
shares_outstandings


df_ShareOutstanding = pd.DataFrame(shares_outstandings)
df_ShareOutstanding


# ---
# # Statements of Income (P/L)

PL = []


# ## Revenues
def get_pl_revenues(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:Revenues$",
        rf"^{NAMESPACE_GAAP}:RevenueFromContractWithCustomerExcludingAssessedTax$",
        rf"^{NAMESPACE_GAAP}:RealEstateRevenueNet$",
        rf"^{NAMESPACE_GAAP}:SalesOfRealEstate$",
        rf"^{NAMESPACE_GAAP}:AdvertisingRevenue$",
        rf"^{NAMESPACE_GAAP}:ElectricalGenerationRevenue$",
        rf"^{NAMESPACE_GAAP}:ContractsRevenue$",
        rf"^{NAMESPACE_GAAP}:RevenueMineralSales$",
        rf"^{NAMESPACE_GAAP}:DeferredRevenueCurrent$",
        rf"^{NAMESPACE_GAAP}:HealthCareOrganizationRevenue$",
        rf"^{NAMESPACE_GAAP}:SalesRevenueGoodsGross$",
        rf"^{NAMESPACE_GAAP}:AdmissionsRevenue$",
        rf"^{NAMESPACE_GAAP}:InsuranceServicesRevenue$",
        rf"^{NAMESPACE_GAAP}:BrokerageCommissionsRevenue$",
        rf"^{NAMESPACE_GAAP}:RegulatedAndUnregulatedOperatingRevenue$"
        rf"^{NAMESPACE_GAAP}:OtherAlternativeEnergySalesRevenue$",
        rf"^{NAMESPACE_GAAP}:DeferredRevenue$",
    ]).lower())    
    return represents(
        get_records_for_financial_element_names(soup=soup, names=names),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_REVENUE
    )


PL += get_pl_revenues(soup)


# ## Cost of Revenues
def get_pl_cost_of_revenues(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:CostOfRevenue$",
        rf"^{NAMESPACE_GAAP}:CostOfGoods$",
        rf"^{NAMESPACE_GAAP}:CostOfGoodsAndServicesSold$",
        rf"^{NAMESPACE_GAAP}:ContractRevenueCost$",
        rf"^{NAMESPACE_GAAP}:CostOfServicesLicensesAndServices$"
        rf"^{NAMESPACE_GAAP}:LicenseCosts$"
    ]).lower())
    return represents(get_records_for_financial_element_names(soup=soup, names=names), FS_PL, FS_ELEMENT_REP_OP_COST)


PL += get_pl_cost_of_revenues(soup)


# ## ***___# Gross Profit___***
def get_pl_gross_profit(soup):
    names = f"{NAMESPACE_GAAP}:GrossProfit".lower()
    return represents(get_records_for_financial_element_names(soup=soup, names=names), fs=FS_PL, rep=FS_ELEMENT_REP_GROSS_PROFIT)


PL += get_pl_gross_profit(soup)


# ## Operating Expenses

# ### Research and Development
def get_pl_operating_expense_r_and_d(soup):
    names = f"{NAMESPACE_GAAP}:ResearchAndDevelopmentExpense".lower()
    return represents(get_records_for_financial_element_names(soup, names), fs=FS_PL, rep=FS_ELEMENT_REP_OPEX_RD)

PL += get_pl_operating_expense_r_and_d(soup)


# ### Administrative Expense
def get_pl_operating_expense_selling_adminstrative(soup):
    names = f"{NAMESPACE_GAAP}:SellingGeneralAndAdministrativeExpense"
    return represents(get_records_for_financial_element_names(soup, names), fs=FS_PL, rep=FS_ELEMENT_REP_OPEX_SGA)
    
PL += get_pl_operating_expense_selling_adminstrative(soup)


# ### Other operating expenses
# 
# The total amount of other operating cost and expense items that are associated with the entity's normal revenue producing operation
def get_pl_operating_expense_other(soup):
    names = re.compile("|".join([
        rf"{NAMESPACE_GAAP}:OtherCostAndExpenseOperating",
        rf"{NAMESPACE_GAAP}:OthertCostOfOperatingRevenue"
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

PL += get_pl_operating_expense_other(soup)


# ## ***___# Total Operating Expenses___***
def get_pl_operating_expense_total(soup):
    names = f"{NAMESPACE_GAAP}:OperatingExpenses".lower()
    return represents(get_records_for_financial_element_names(soup=soup, names=names), fs=FS_PL, rep=FS_ELEMENT_REP_OPEX)
PL += get_pl_operating_expense_total(soup)


# ## ***___# Operating Income___***
# 
# ```Operating Income = GrossProfit - Total Operating Expenses```
def get_pl_operating_income(soup):
    names = f"{NAMESPACE_GAAP}:OperatingIncomeLoss".lower()
    return represents(get_records_for_financial_element_names(soup=soup, names=names), fs=FS_PL, rep=FS_ELEMENT_REP_OP_INCOME)


PL += get_pl_operating_income(soup)


# ## Non Operating Expenses

# ### Interest Expense
# 
# * [Investopedia - What Is an Interest Expense?](https://www.investopedia.com/terms/i/interestexpense.asp)
# 
# > An interest expense is the cost incurred by an entity for borrowed funds. Interest expense is a non-operating expense shown on the income statement. It represents interest payable on any borrowings – bonds, loans, convertible debt or lines of credit. It is essentially calculated as the interest rate times the outstanding principal amount of the debt. Interest expense on the income statement represents ***interest accrued during the period*** covered by the financial statements, and **NOT the amount of interest paid over that period**. While interest expense is tax-deductible for companies, in an individual's case, it depends on his or her jurisdiction and also on the loan's purpose.  
# >
# > For most people, mortgage interest is the single-biggest category of interest expense over their lifetimes as interest can total tens of thousands of dollars over the life of a mortgage as illustrated by online calculators.
def get_pl_non_operating_expense_interest(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:InterestExpense$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

PL += get_pl_non_operating_expense_interest(soup)


# ### Non-operating Expenses
def get_pl_non_operatintg_expense_other(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:OtherNonOperatingIncomeExpense$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

PL += get_pl_non_operating_expense_interest(soup)


# ## Income Tax
def get_pl_income_tax(soup):
    names = f"{NAMESPACE_GAAP}:IncomeTaxExpenseBenefit"
    return get_records_for_financial_element_names(soup=soup, names=names)

PL += get_pl_income_tax(soup)


# ## ***___# Net Income___***
# 
# $GrossProfit - (Operating Expenses + NonOperating Expense) - Tax$
def get_pl_net_income(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:NetIncomeLoss$",
        rf"^{NAMESPACE_GAAP}:ProfitLoss$",
    ]).lower())
    return represents(get_records_for_financial_element_names(soup=soup, names=names), fs=FS_PL, rep=FS_ELEMENT_REP_NET_INCOME)


PL += get_pl_net_income(soup)


# ## ***___# Shares Outstandings___***
PL += represents(shares_outstandings, fs=FS_PL, rep=FS_ELEMENT_REP_SHARES_OUTSTANDING)


# ## ***___# Net Income Per Share___***
# 
# * [US GAAP - Is Net Income Per Share the same with EPS?](https://money.stackexchange.com/questions/148015/us-gaap-is-net-income-per-share-the-same-with-eps)
def get_pl_eps(soup):
    return get_records_for_financial_element_names(
        soup=soup, names=f"{NAMESPACE_GAAP}:EarningsPerShareBasic".lower()
    ) + \
    get_records_for_financial_element_names(
        soup=soup, names=f"{NAMESPACE_GAAP}:EarningsPerShareBasicAndDiluted".lower()
    )


# In[58]:


PL += represents(get_pl_eps(soup), fs=FS_PL, rep=FS_ELEMENT_REP_EPS)


# ## Display P/L
# 
# Is ```us-gaap:othernonoperatingincomeexpense``` credit or debit? As the value is **negative** and so is in the Income Statement, is shoudl be credit -> To be confirmed. 
df_PL = pd.DataFrame(PL, columns=get_financial_element_columns())
credits = df_PL[df_PL['type'] == 'credit']['value'].sum()
credits


debits = df_PL[df_PL['type'] == 'debit']['value'].sum()
credits - debits  # Equal to the Net Income


# ### EPS
shares = df_PL[df_PL['rep'] == FS_ELEMENT_REP_SHARES_OUTSTANDING]
if len(shares) == 1:
    num_shares = shares['value'].values.item()
else:
    assert False, f"No Shares OutStanding row found {df_PL}"


shares = df_PL[df_PL['rep'] == FS_ELEMENT_REP_NET_INCOME]
if len(shares) == 1:
    net_income = shares['value'].item()
else:
    assert False, f"No Net Income row found {df_PL}"

eps = net_income / num_shares
scale = 2
print(f"{eps:.{scale}f}")


# ---
# # Balance Sheet (B/S)
BS = []


# ## Cash & Cash Equivalents
# Look for the cash and cash equivalents for the reporting perid in the Balance Sheet and Cash Flow statements of the  10-K.
def get_bs_current_asset_cash_and_equivalents(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:CashAndCashEquivalentsAtCarryingValue$",
    ]).lower())
    return represents(get_records_for_financial_element_names(soup=soup, names=names), fs=FS_BS, rep=FS_ELEMENT_REP_CASH)

BS += get_bs_current_asset_cash_and_equivalents(soup)


# ## Restricted Cash
def get_bs_current_asset_restricted_cash_and_equivalents(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:RestrictedCashEquivalentsCurrent$",
        rf"^{NAMESPACE_GAAP}:RestrictedCashAndCashEquivalentsAtCarryingValue$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_current_asset_restricted_cash_and_equivalents(soup)


# ## Short Term Investments
def get_bs_current_asset_short_term_investments(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:ShortTermInvestments$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_current_asset_short_term_investments(soup)


# ## Account Receivable
def get_bs_current_asset_account_receivables(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:ReceivablesNetCurrent$",
        rf"^{NAMESPACE_GAAP}:AccountsReceivableNetCurrent$",
        rf"^{NAMESPACE_GAAP}:OtherReceivables$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

PL += get_bs_current_asset_account_receivables(soup)


# ## ***___Inventory___***
def get_bs_current_asset_inventory(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:InventoryNet$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

PL += get_bs_current_asset_inventory(soup)


# ## Prepaid Expense / Other Assets Current
# 
# * [Understanding Prepaid Expenses](https://www.investopedia.com/terms/p/prepaidexpense.asp)
# 
# > Companies make prepayments for goods or services such as leased office equipment or insurance coverage that provide continual benefits over time. Goods or services of this nature cannot be expensed immediately because the expense would not line up with the benefit incurred over time from using the asset.  
# >
# > According to generally accepted accounting principles (GAAP), expenses should be recorded in the same accounting period as the benefit generated from the related asset.
# 
# * [us-gaap: PrepaidExpenseAndOtherAssetsCurrent](https://www.calcbench.com/element/PrepaidExpenseAndOtherAssetsCurrent)
# 
# > Amount of asset related to consideration paid in advance for costs that provide economic benefits in future periods, and amount of other assets that are expected to be realized or consumed within one year or the normal operating cycle, if longer.
# 
# * [Other Current Assets (OCA)](https://www.investopedia.com/terms/o/othercurrentassets.asp)
# 
# > Other current assets (OCA) is a category of things of value that a company owns, benefits from, or uses to generate income that can be converted into cash within one business cycle. They are referred to as “other” because they are uncommon or insignificant, unlike typical current asset items such as cash, securities, accounts receivable, inventory, and prepaid expenses.
def get_bs_current_asset_other(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:PrepaidExpenseCurrent$",
        rf"^{NAMESPACE_GAAP}:PrepaidExpenseAndOtherAssetsCurrent$",
        rf"^{NAMESPACE_GAAP}:OperatingLeaseRightOfUseAsset$",
        rf"^{NAMESPACE_GAAP}:OtherAssetsCurrent$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_current_asset_other(soup)


# ## ***___# Current Assets___***
def get_bs_current_assets(soup):
    names = re.compile("|".join([
    rf"^{NAMESPACE_GAAP}:AssetsCurrent$",
    ]).lower())
    return represents(get_records_for_financial_element_names(soup=soup, names=names), fs=FS_BS, rep=FS_ELEMENT_REP_CURRENT_ASSETS)

BS += get_bs_current_assets(soup)    


# ## Property, Plant, Equipment
def get_bs_non_current_asset_property_and_equipment(soup):
    names = re.compile("|".join([
    rf"^{NAMESPACE_GAAP}:PropertyPlantAndEquipmentNet$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_asset_property_and_equipment(soup)


## Restricted Cash Non Current
def get_bs_non_current_asset_restricted_cash_and_equivalent(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:RestrictedCashAndCashEquivalentsNoncurrent$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_asset_restricted_cash_and_equivalent(soup)


# ## Deferred Tax
def get_bs_non_current_asset_deferred_income_tax(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:DeferredIncomeTaxAssetsNet$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_asset_deferred_income_tax(soup)


# ## ***___GoodWill___***
def get_bs_non_current_asset_goodwill(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:GoodWill$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_asset_goodwill(soup)


# ## Intangible and Other Assets
def get_bs_non_current_asset_other(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:IntangibleAssetsNetExcludingGoodwill$",
        rf"^{NAMESPACE_GAAP}:OtherAssetsNoncurrent$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_asset_other(soup)


# ## ***___# Total Assets___***
def get_bs_total_assets(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:Assets$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += represents(get_bs_total_assets(soup), fs=FS_BS, rep=FS_ELEMENT_REP_TOTAL_ASSETS)


# ## Account Payable
# 
# * [Accounts Payable (AP)](https://www.investopedia.com/terms/a/accountspayable.asp)
# 
# > company's obligation to pay off a short-term debt to its creditors or suppliers.
# 
# 
# 
# * [Accrued Liability](https://www.investopedia.com/terms/a/accrued-liability.asp) (売掛金)
# 
# > costs for goods and services already delivered to a company for which it must pay in the future. A company can accrue liabilities for any number of obligations and are recorded on the company's balance sheet. They are normally listed on the balance sheet as current liabilities and are adjusted at the end of an accounting period.
# 
# 
# * [us-gaap:AccruedLiabilitiesCurrent](http://xbrlsite.azurewebsites.net/2019/Prototype/references/us-gaap/Element-354.html)
def get_bs_current_liability_account_payable(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:AccountsPayableCurrent$",
        rf"^{NAMESPACE_GAAP}:AccruedLiabilitiesCurrent$",
        rf"^{NAMESPACE_GAAP}:CapitalExpendituresIncurredButNotYetPaid$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_current_liability_account_payable(soup)


# ## Tax
def bs_get_current_liability_tax(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:TaxesPayableCurrent$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += bs_get_current_liability_tax(soup)


# ## Debt Due
def bs_get_current_liability_longterm_debt(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:LongTermDebtCurrent$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += bs_get_current_liability_longterm_debt(soup)


# ## ***___# Current Liabilities___***
def get_bs_current_liabilities(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:LiabilitiesCurrent$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += represents(get_bs_current_liabilities(soup), fs=FS_BS, rep=FS_ELEMENT_REP_CURRENT_LIABILITIES)


# ## Long Term Debt
def get_bs_non_current_liability_longterm_debt(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:LongTermDebt$",
        rf"^{NAMESPACE_GAAP}:LongTermDebtNoncurrent$",
        rf"^{NAMESPACE_GAAP}:LongTermLoansFromBank$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_liability_longterm_debt(soup)


# ## Tax Deferred
def get_bs_non_current_liability_deferred_tax(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:DeferredIncomeTaxLiabilitiesNet$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_liability_deferred_tax(soup)


# ## Other Long Term Liabilities
# 
# * [Postemployment Benefits Liability, Noncurrent/us-gaap:PostemploymentBenefitsLiabilityNoncurrent](http://xbrlsite.azurewebsites.net/2019/Prototype/references/us-gaap/Element-12380.html)
# 
# > The obligations recognized for the various benefits provided to former or inactive employees, their beneficiaries, and covered dependents after employment but before retirement that is payable after one year (or beyond the operating cycle if longer).
def get_bs_non_current_liability_other(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:PostemploymentBenefitsLiabilityNoncurrent$",
        rf"^{NAMESPACE_GAAP}:PensionAndOtherPostretirementAndPostemploymentBenefitPlansLiabilitiesNoncurrent$",
        rf"^{NAMESPACE_GAAP}:OperatingLeaseLiabilityNoncurrent$",
        rf"^{NAMESPACE_GAAP}:OtherLiabilitiesNoncurrent$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_non_current_liability_other(soup)


# ## ***___# Total Liabilities___***
def get_bs_total_liabilities(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:Liabilities$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += represents(get_bs_total_liabilities(soup), fs=FS_BS, rep=FS_ELEMENT_REP_LIABILITIES)


# ## Paid-in Capital
# 
# * [Paid-In Capital](https://www.investopedia.com/terms/p/paidincapital.aspPaid-In Capital)
# 
# > Paid-in capital represents the funds raised by the business through selling its equity and not from ongoing business operations. Paid-in capital also refers to a line item on the company's balance sheet listed under shareholders' equity (also referred to as stockholders' equity), often shown alongside the line item for additional paid-in capital.
# 
# * [Additional Paid-In Capital (APIC)](https://www.investopedia.com/terms/a/additionalpaidincapital.asp)
# 
# > Often referred to as "contributed capital in excess of par,” APIC occurs when an investor buys newly-issued shares directly from a company during its initial public offering (IPO) stage. APIC, which is itemized under the shareholder equity (SE) section of a balance sheet, is viewed as a profit opportunity for companies as it results in them receiving excess cash from stockholders.
def get_bs_stockholders_equity_paid_in(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:CommonStockValue$",
        rf"^{NAMESPACE_GAAP}:AdditionalPaidInCapitalCommonStock$",
        rf"^{NAMESPACE_GAAP}:CommonStocksIncludingAdditionalPaidInCapital$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_stockholders_equity_paid_in(soup)


# ## Retained Earnings
# 
# * [Retained Earnings](https://www.investopedia.com/terms/r/retainedearnings.asp)
# 
# >  The word "retained" captures the fact that because those earnings were **NOT paid out to shareholders as dividends** they were instead retained by the company.
def get_bs_stockholders_equity_retained(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:RetainedEarningsAppropriated$",
        rf"^{NAMESPACE_GAAP}:RetainedEarningsAccumulatedDeficit$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_stockholders_equity_retained(soup)


# ## Accumulated other comprehensive income/loss
def get_bs_stockholders_equity_other(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:AccumulatedOtherComprehensiveIncomeLossNetOfTax$",
        rf"^{NAMESPACE_GAAP}:MinorityInterest$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += get_bs_stockholders_equity_other(soup)


# ## ***___# Stockholder's Equity___***
# 
# * [Stockholders' Equity](https://www.investopedia.com/terms/s/stockholdersequity.asp)
# 
# > Remaining amount of assets available to shareholders after all liabilities have been paid. (純資産)
def get_bs_stockholders_equity(soup):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:StockholdersEquity$",
        rf"^{NAMESPACE_GAAP}:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest$",
    ]).lower())
    return get_records_for_financial_element_names(soup=soup, names=names)

BS += represents(get_bs_stockholders_equity(soup), fs=FS_BS, rep=FS_ELEMENT_REP_TOTAL_EQUITY)


# ## ***___# Total Liabilities + Stockholder's Equity___***
names = re.compile("|".join([
    rf"^{NAMESPACE_GAAP}:LiabilitiesAndStockholdersEquity$",
]).lower())
BS += get_records_for_financial_element_names(soup=soup, names=names)


# ## Display B/S
df_BS = pd.DataFrame(BS, columns=get_financial_element_columns())
#df_BS = df_BS.style.set_properties(**{'text-align': 'left'})
df_BS[(df_BS['rep'].notna()) & (df_BS['rep'] != "")]


credits = df_BS[df_BS['type'] == 'credit']['value'].sum()
debits = df_BS[df_BS['type'] == 'debit']['value'].sum()

# ### Cash Per Share
cash = df_BS[df_BS['rep'] == FS_ELEMENT_REP_CASH]['value'].values.item()
cps = cash / num_shares

scale = 2
print(f"{cps:.{scale}f}")


# ### EPS VS CPS
if (cps / eps) > 1.0:
    print(f"Saving is {cps/eps:.2f} more than earning")


# # Save

import pathlib
path_to_folder = f"../data/{CIK}_{registrant_name}"
pathlib.Path(f"{path_to_folder}").mkdir(parents=True, exist_ok=True)
df_PL.to_csv(f"{path_to_folder}/{ACCESSION}_PL.gz", index=False, compression='gzip')
df_BS.to_csv(f"{path_to_folder}/{ACCESSION}_BS.gz", index=False, compression='gzip')
