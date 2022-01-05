"""
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
    List,
    Callable
)

import bs4
import dateutil
import numpy as np
from bs4 import BeautifulSoup

from sec_edgar_constant import (
    # SEC Form Types
    SEC_FORM_TYPE_10K,
    SEC_FORM_TYPE_10Q,
    # EDGAR
    # Financial statement types
    FS_PL,
    # Financial Statement element types
    FS_ELEMENT_TYPE_CREDIT,
    FS_ELEMENT_TYPE_DEBIT,
    FS_ELEMENT_TYPE_FACT,
    FS_ELEMENT_TYPE_CALC,
    FS_ELEMENT_TYPE_METRIC,
    #
    # PL
    FS_ELEMENT_REP_REVENUE,
    # BS
)
from xbrl_gaap_constant import (
    NAMESPACE_GAAP,
    GAAP_DEBIT_ITEMS,
    GAAP_CREDIT_ITEMS,
    GAAP_FACT_ITEMS,
    GAAP_CALC_ITEMS,
    GAAP_METRIC_ITEMS,
)

REGEXP_NUMERIC = re.compile(r"\s*[\d.-]*\s*")
PL_FUNCTIONS: List[Callable] = [

]
BS_FUNCTIONS: List[Callable] = [

]


# ================================================================================
# Utility
# ================================================================================
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


# ================================================================================
# XBRL/US-GAAP taxonomy handlers
# ================================================================================
def get_company_name(soup):
    """Get company (registrant) name from the XBRL"""
    registrant_name = soup.find("dei:EntityRegistrantName".lower()).string.strip()
    assert registrant_name, f"No registrant name found"

    # Remove non-ascii characters to form a company name
    registrant_name = ''.join(e for e in registrant_name if e.isalnum())
    logging.debug("get_company_name(): company name is [%s]" % registrant_name)
    return registrant_name


# --------------------------------------------------------------------------------
# # Repoting period
# Each 10-K and 10-Q XBRL has the reporting period for the filing.
# To exclude other periods, e.g. previous year or quarter, use the **context id**`
# for the reporting period. **Most** 10-K, 10-Q specify the annual period with
# the first ```<startDate> and <endDate>``` tags.
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
def get_report_period_end_date(soup):
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


def get_target_context_ids(soup, period_end_date: str, form_type: str):
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


def get_regexp_to_match_xbrl_contexts(soup, form_type: str) -> re.Pattern:
    """Generate the regexp to match the ContextRef attribute values

    XBRL (10-K,10-Q) uses multiple contexts to refer to the F/S items in the
    reporting period.

    For instance, the reporting period context has the Context ID=
    "ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403" for the 10-K
    reporting period (fiscal year ended April 3, 2021).

    <context id="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403">
        <entity>
        <identifier scheme="http://www.sec.gov/CIK">0001604778</identifier>
        </entity>
        <period>
            <startDate>2020-03-29</startDate>
            <endDate>2021-04-03</endDate>         # <--- end of the period
        </period>
    </context>

    All the elements whose ContextRef value matches with the Context ID
    belongs to the reporting period.

    * Revenues for Fiscal 2021 has the context ID of the period.
    <us-gaap:Revenues
        contextRef="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403" <---
        decimals="-3"
        id=...
        unitRef="usd"
    >
    4015307000
    </us-gaap:Revenues>

    To be able to find the FS elements that belongs to the report period of
    the filing, first we need to find out all the Context IDs that refer to
    the current reporting period.

    Then we need to check each FS element if its ContextRef matches with
    one of the Context IDs.

    Args:
        soup: BS4 source
        form_type: Filing form type e.g. FORM_TYPE_10K
    """
    # --------------------------------------------------------------------------------
    # Find the end date of the reporting period.
    # <context id="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403">
    #     <entity>
    #     <identifier scheme="http://www.sec.gov/CIK">0001604778</identifier>
    #     </entity>
    #     <period>
    #         <startDate>2020-03-29</startDate>
    #         <endDate>2021-04-03</endDate>         # <--- end of the period
    #     </period>
    # </context>
    # --------------------------------------------------------------------------------
    report_period_end_date = get_report_period_end_date(soup)

    # --------------------------------------------------------------------------------
    # Get all the Context IDs that belong to the filing reporting period.
    # Form Type (10-K, 10-Q) is required to narrow down the range within which
    # the start date of the period should. If it is 10-K with end date 2021-04-03,
    # the start date should be in-between the two:
    # 2020-03-04 (end_date - duration - datetime.timedelta(days=30) and
    # 2020-05-03 (end_date - duration + datetime.timedelta(days=30).
    # --------------------------------------------------------------------------------
    context_ids = get_target_context_ids(
        soup=soup, period_end_date=report_period_end_date, form_type=form_type
    )
    regexp = re.compile("|".join(context_ids))
    return regexp


def find_financial_elements(soup, element_names, attributes):
    """Find the financial statement elements from the XML/HTML source.
    Args:
        soup: BS4 source
        element_names: String or regexp instance to select the financial elements.
        attributes: tag attributes to select the financial elements
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


def get_record_for_nil_elements(elements):
    return []


def get_records_for_financial_elements(elements):
    """Financial record having the columns of get_financial_element_columns"""
    # assert isinstance(elements, bs4.element.ResultSet) or isinstance(elements[0], bs4.element.Tag)
    assert_bf4_tag(elements[0])

    results = []
    for element in elements:
        # F/S
        element_fs = ""

        # Rep
        element_rep = ""

        # Type of the element
        element_type = None
        if element.name in GAAP_DEBIT_ITEMS: element_type = FS_ELEMENT_TYPE_DEBIT
        if element.name in GAAP_CREDIT_ITEMS: element_type = FS_ELEMENT_TYPE_CREDIT
        if element.name in GAAP_CALC_ITEMS: element_type = FS_ELEMENT_TYPE_CALC
        if element.name in GAAP_METRIC_ITEMS: element_type = FS_ELEMENT_TYPE_METRIC
        if element.name in GAAP_FACT_ITEMS: element_type = FS_ELEMENT_TYPE_FACT
        assert element_type is not None, \
            "Cannot identify the type for the element name [%s]" % element.name

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
    if len(records) > 0:
        row = records[0]
        row[0] = fs
        row[1] = rep

    return records


def get_records_for_financial_element_names(soup, names, attributes: dict):
    """Get financial records that match the financial element names
    """
    elements = find_financial_elements(soup=soup, element_names=names, attributes=attributes)
    if len(elements) > 0:
        display_elements(elements)
        return get_records_for_financial_elements(elements)
    else:
        return get_record_for_nil_elements(elements)


# In[34]:


def get_values_for_financial_element_names(soup, names , attributes: dict):
    elements = find_financial_elements(soup=soup, element_names=names, attributes=attributes)
    if len(elements) > 0:
        display_elements(elements)
        return get_financial_element_numeric_values(elements)
    else:
        return []


# ---
# # Shares Outstanding
def get_shares_outstanding(soup, attributes: dict):
    names = re.compile("|".join([
        rf"{NAMESPACE_GAAP}:SharesOutstanding",
        rf"{NAMESPACE_GAAP}:CommonStockSharesOutstanding",
        rf"{NAMESPACE_GAAP}:CommonStockOtherSharesOutstanding",
    ]).lower())

    return get_records_for_financial_element_names(soup=soup, names=names, attributes=attributes)


# ================================================================================
# P/L
# ================================================================================
def get_pl_revenues(soup, attributes: dict):
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
        get_records_for_financial_element_names(soup=soup, names=names, attributes=attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_REVENUE
    )
