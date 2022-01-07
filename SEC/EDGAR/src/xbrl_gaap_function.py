"""
Functions to extract the XBRL XML elements of us-GAAP
"""
import datetime
import logging
import re

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
    FS_BS,
    # Financial Statement element types
    FS_ELEMENT_TYPE_CREDIT,
    FS_ELEMENT_TYPE_DEBIT,
    FS_ELEMENT_TYPE_FACT,
    FS_ELEMENT_TYPE_CALC,
    FS_ELEMENT_TYPE_METRIC,
    # PL
    FS_ELEMENT_REP_REVENUE,
    FS_ELEMENT_REP_OP_COST,
    FS_ELEMENT_REP_OP_INCOME,
    FS_ELEMENT_REP_GROSS_PROFIT,
    FS_ELEMENT_REP_OPEX_RD,
    FS_ELEMENT_REP_OPEX_SGA,
    FS_ELEMENT_REP_OPEX,
    FS_ELEMENT_REP_NET_INCOME,
    # BS
    FS_ELEMENT_REP_CASH,
    FS_ELEMENT_REP_CURRENT_ASSETS,
    FS_ELEMENT_REP_TOTAL_ASSETS,
    FS_ELEMENT_REP_CURRENT_LIABILITIES,
    FS_ELEMENT_REP_LIABILITIES,
    FS_ELEMENT_REP_EQUITY,
    FS_ELEMENT_REP_SHARES_OUTSTANDING,
    FS_ELEMENT_REP_EQUITY_AND_LIABILITIES,
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


# ================================================================================
# Utility
# ================================================================================
def assert_bf4_tag(element):
    assert isinstance(element, bs4.element.Tag), f"Expected BS4 tag but {element} of type {type(element)}"


def display_elements(elements):
    assert isinstance(elements, bs4.element.ResultSet) or isinstance(elements[0], bs4.element.Tag)
    for element in elements:  # decimals="-3" means the displayed value is divied by 1000.
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
    registrant_name = soup.find(
        name=re.compile("dei:EntityRegistrantName", re.IGNORECASE)
    ).string.strip()
    assert registrant_name, f"No registrant name found"

    # Remove non-ascii characters to form a company name
    registrant_name = ''.join(e for e in registrant_name if e.isalnum())
    logging.debug("get_company_name(): company name is [%s]" % registrant_name)
    return registrant_name


# --------------------------------------------------------------------------------
# # Reporting period
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
def get_report_period_end_date(soup) -> str:
    """Identify the end date of the report period from the first "context" tag
    in the XBRL that has <period><startDate> tag as its child tag.

    [Assumption]
    Context identifies a date which can be current period or previous
    because FS has both e.g. 2020 is current but 2019 is there as well.
    In addition, multiple contexts can be used to refer to the FS elements
    in the same period.

    Hence, we need to identify which context defines the start date and
    end date of the filing.

    There is no clearly defined rule to identify, hence assume that the
    first Context that appears in the XBRL XML is the one.

    [Example]
    <context id="ifb6ce67cf6954ebf88471dd82daa9247_D20200329-20210403">
        <entity>
        <identifier scheme="http://www.sec.gov/CIK">0001604778</identifier>
        </entity>
        <period>
            <startDate>2020-03-29</startDate>
            <endDate>2021-04-03</endDate>        # <----- period end date
        </period>
    </context>

    Args:
        soup: Source BS4
    Returns: reporting period in string e.g. "2021-09-30"
    Raises:
        RuntimeError: when the report period date is invalid
    """
    first_context = None
    for context in soup.find_all('context'):
        if context.find('period') and context.find('period').find('enddate'):
            first_context = context
            break

    # --------------------------------------------------------------------------------
    # The end date of the current report period is mandatory, without which
    # we cannot process the filing XBRL.
    # --------------------------------------------------------------------------------
    if first_context is None:
        raise RuntimeError("No Context found to identify the current report period end date.")
    end_date = first_context.find('period').find('enddate').text.strip()

    # --------------------------------------------------------------------------------
    # Validate date/time string format
    # --------------------------------------------------------------------------------
    try:
        dateutil.parser.parse(end_date)
    except ValueError as e:
        logging.error(
            "get_report_period_end_date(): invalid end date[%s] in context\n[%s]" %
            (end_date, first_context)
        )
        raise RuntimeError("get_report_period_end_date()") from e

    return end_date


def get_target_context_ids(soup, period_end_date: str, form_type: str):
    """
    Extract all the contexts that refer to the reporting period of the filing.

    XBRL (10-K,10-Q) uses multiple contexts to refer to the F/S items in the
    reporting period. Some Contexts refer to the current period but others
    refer to previous. Hence, need to identify the Contexts that refer to the
    current only.

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
        duration = datetime.timedelta(days=90)
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


def get_regexp_to_match_target_context_ids(soup, form_type: str) -> re.Pattern:
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
    # Identify the END_DATE of the report period from the XBRL XML.
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


def get_attributes_to_select_target_fs_elements(
        soup: bs4.BeautifulSoup,
        form_type: str
) -> dict:
    """Generate dictionary of attributes by which to select the FS elements
    that belong to the reporting period of the filing.

    For instance, to be able to select the revenue element:
    <us-gaap:revenues
        contextref="XYZ"
        decimals="-3"
        unitRef="usd"
        id=...
    >
    4015307000
    </us-gaap:Revenues>

    We need the attributes = {
        "contextref": regexp("XYZ|123|..."),
        "decimals": True,
        "unitref": True
    }

    SELECT elements FROM XML
    WHERE
        regexp("XYZ|123|...").match(contextref)
        AND decimals is True
        AND unitref is True

    Args:
        soup: XML source
        form_type: SEC Form Type
    """
    regexp_to_match_target_context_ids = get_regexp_to_match_target_context_ids(
        soup=soup, form_type=form_type
    )
    attributes = {
        "contextref": regexp_to_match_target_context_ids,
        "decimals": True,
        "unitref": True
    }
    return attributes


def find_financial_elements(soup, element_names: re.Pattern, attributes: dict):
    """Find the financial statement elements from the XML/HTML source.
    Args:
        soup: BS4 source
        element_names: String or regexp instance to select the financial elements.
        attributes: tag attributes to select the financial elements
    Returns:
        List of BS4 tag objects that matched the element_names and attributes.
    """
    assert isinstance(soup, BeautifulSoup)
    # assert isinstance(element_names, re.Pattern) or isinstance(element_names, str)
    assert isinstance(element_names, re.Pattern)

    # element_names = element_names.lower() if isinstance(element_names, str) else element_names
    elements = soup.find_all(
        name=element_names,
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
        "FS",  # Which financial statement e.g. bs for Balance Sheet
        "Rep",  # Representative marker e.g. "income" or "cogs" (cost of goods sold)
        "Type",  # "debit" or "credit"
        "Name",  # F/S item name, e.g. us-gaap:revenues
        "Value",  # Account value
        "Unit",  # e.g. USD
        "Decimals",  # Scale
        "Context"  # XBRL context ID
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
    """Mark the row with the string that tells what the row is about.
    To mark a row that "this row is about (represents) 'revenue'", then set
    "revenue" in the second column (Rep) of the first record.

    [Requirement]
    The record has the format |FS|Rep|Type|Name|Value|Unit|Decimals|Context|

    Args:
        records: FS elements that belongs to the FS category to be represented
        by the marker. For instance, "revenue" can be reported using different
        FS element depending on the industry. Real estate business may report
        its total revenue as SalesOfRealEstate, or Insurance business may report
        it as InsuranceServicesRevenue.

        Regardless with which FS element is used, we would like to identify the
        single FS element in the SEC filing XBRL XML file that represents
        "Total Revenue of the company for the filing period".

        fs: Type of Financial Statement e.g. "bs" for balance sheet.
        rep: String to represent the FS category, e.g "revenue".

    Returns: List of record, first of which is marked with the fs/rep.
    """
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


def get_values_for_financial_element_names(soup, names, attributes: dict):
    elements = find_financial_elements(soup=soup, element_names=names, attributes=attributes)
    if len(elements) > 0:
        display_elements(elements)
        return get_financial_element_numeric_values(elements)
    else:
        return []


def get_shares_outstanding(soup, attributes: dict):
    names = re.compile("|".join([
        rf"{NAMESPACE_GAAP}:SharesOutstanding",
        rf"{NAMESPACE_GAAP}:CommonStockSharesOutstanding",
        rf"{NAMESPACE_GAAP}:CommonStockOtherSharesOutstanding",
    ]), re.IGNORECASE)
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
        ]),
        re.IGNORECASE
    )
    return represents(
        get_records_for_financial_element_names(soup=soup, names=names, attributes=attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_REVENUE
    )


def get_pl_cost_of_revenues(soup, attributes: dict):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:CostOfRevenue$",
        rf"^{NAMESPACE_GAAP}:CostOfGoods$",
        rf"^{NAMESPACE_GAAP}:CostOfGoodsAndServicesSold$",
        rf"^{NAMESPACE_GAAP}:ContractRevenueCost$",
        rf"^{NAMESPACE_GAAP}:CostOfServicesLicensesAndServices$"
        rf"^{NAMESPACE_GAAP}:LicenseCosts$"
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(soup=soup, names=names, attributes=attributes),
        FS_PL,
        FS_ELEMENT_REP_OP_COST
    )


def get_pl_gross_profit(soup, attributes: dict):
    names = re.compile(f"^{NAMESPACE_GAAP}:GrossProfit$", re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(soup=soup, names=names, attributes=attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_GROSS_PROFIT
    )


# --------------------------------------------------------------------------------
# P/L Operating Expenses
# --------------------------------------------------------------------------------
def get_pl_operating_expense_r_and_d(soup, attributes: dict):
    """Get the R&D expense"""
    names = re.compile(
        rf"^{NAMESPACE_GAAP}:ResearchAndDevelopmentExpense$", re.IGNORECASE
    )
    return represents(
        get_records_for_financial_element_names(soup, names, attributes=attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_OPEX_RD
    )


def get_pl_operating_expense_selling_administrative(soup, attributes: dict):
    """Get the Administrative Expense (HanKanHi in JP)"""
    names = re.compile(
        rf"^{NAMESPACE_GAAP}:SellingGeneralAndAdministrativeExpense$", re.IGNORECASE
    )
    return represents(
        get_records_for_financial_element_names(soup, names, attributes=attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_OPEX_SGA
    )


def get_pl_operating_expense_other(soup, attributes: dict):
    """
    Get the total amount of other operating cost and expense items that
    are associated with the entity's normal revenue producing operation
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:OtherCostAndExpenseOperating$",
        rf"^{NAMESPACE_GAAP}:OthertCostOfOperatingRevenue$"
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes)


def get_pl_operating_expense_total(soup, attributes: dict):
    """Get the Total Operating Expenses"""
    names = re.compile(
        rf"^{NAMESPACE_GAAP}:OperatingExpenses$", re.IGNORECASE
    )
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_OPEX)


def get_pl_operating_income(soup, attributes: dict):
    """Operating Income = GrossProfit - Total Operating Expenses"""
    names = re.compile(
        f"^{NAMESPACE_GAAP}:OperatingIncomeLoss$", re.IGNORECASE
    )
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_OP_INCOME
    )


# --------------------------------------------------------------------------------
# P/L Non Operating Expenses
# --------------------------------------------------------------------------------
def get_pl_non_operating_expense_interest(soup, attributes: dict):
    """Get Interest Expense
    * [Investopedia - What Is an Interest Expense?]
    (https://www.investopedia.com/terms/i/interestexpense.asp)
    > An interest expense is the cost incurred by an entity for borrowed funds.
    > Interest expense is a non-operating expense shown on the income statement.
    > It represents interest payable on any borrowings – bonds, loans, convertible
    > debt or lines of credit. It is essentially calculated as the interest rate
    > times the outstanding principal amount of the debt.
    >
    > Interest expense on the income statement represents ***interest accrued
    > during the period*** covered by the financial statements, and **NOT the amount
    > of interest paid over that period**. While interest expense is tax-deductible
    > for companies, in an individual's case, it depends on his or her jurisdiction
    > and also on the loan's purpose.
    > For most people, mortgage interest is the single-biggest category of interest
    > expense over their lifetimes as interest can total tens of thousands of dollars
    > over the life of a mortgage as illustrated by online calculators.
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:InterestExpense$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_pl_non_operating_expense_other(soup, attributes: dict):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:OtherNonOperatingIncomeExpense$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_pl_income_tax(soup, attributes: dict):
    """Income Tax"""
    names = re.compile(
        f"^{NAMESPACE_GAAP}:IncomeTaxExpenseBenefit$", re.IGNORECASE
    )
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_pl_net_income(soup, attributes: dict):
    """
    Net Income = GrossProfit - (Operating Expenses + NonOperating Expense) - Tax
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:NetIncomeLoss$",
        rf"^{NAMESPACE_GAAP}:ProfitLoss$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_NET_INCOME
    )


def get_pl_shares_outstanding(soup, attributes: dict):
    return represents(
        get_shares_outstanding(soup, attributes),
        fs=FS_PL,
        rep=FS_ELEMENT_REP_SHARES_OUTSTANDING
    )


def get_pl_eps(soup, attributes: dict):
    """[US GAAP - Is Net Income Per Share the same with EPS?]
    (https://money.stackexchange.com/questions/148015)
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:EarningsPerShareBasic$",
        rf"^{NAMESPACE_GAAP}:EarningsPerShareBasicAndDiluted$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup,
        names=names,
        attributes=attributes
    )


# ================================================================================
# B/S
# ================================================================================
# --------------------------------------------------------------------------------
# Current Assets
# --------------------------------------------------------------------------------
def get_bs_current_asset_cash_and_equivalents(soup, attributes: dict):
    """Cash & Cash EquivalentsCash & Cash Equivalents
    Look for the cash and cash equivalents for the reporting period
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:CashAndCashEquivalentsAtCarryingValue$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_BS,
        rep=FS_ELEMENT_REP_CASH
    )


def get_bs_current_asset_restricted_cash_and_equivalents(soup, attributes: dict):
    """Restricted Cash"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:RestrictedCashEquivalentsCurrent$",
        rf"^{NAMESPACE_GAAP}:RestrictedCashAndCashEquivalentsAtCarryingValue$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_current_asset_short_term_investments(soup, attributes: dict):
    """Short Term Investments"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:ShortTermInvestments$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_current_asset_account_receivables(soup, attributes: dict):
    """Account Receivable"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:ReceivablesNetCurrent$",
        rf"^{NAMESPACE_GAAP}:AccountsReceivableNetCurrent$",
        rf"^{NAMESPACE_GAAP}:OtherReceivables$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_current_asset_inventory(soup, attributes: dict):
    """Inventory"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:InventoryNet$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


# --------------------------------------------------------------------------------
# ## Prepaid Expense / Other Assets Current
#
# * [Understanding Prepaid Expenses]
# (https://www.investopedia.com/terms/p/prepaidexpense.asp)
#
# > Companies make prepayments for goods or services such as leased office
# > equipment or insurance coverage that provide continual benefits over time.
# > Goods or services of this nature cannot be expensed immediately because the
# > expense would not line up with the benefit incurred over time from using
# > the asset.
# >
# > According to generally accepted accounting principles (GAAP), expenses
# > should be recorded in the same accounting period as the benefit generated
# > from the related asset.
#
# * [us-gaap: PrepaidExpenseAndOtherAssetsCurrent]
# (https://www.calcbench.com/element/PrepaidExpenseAndOtherAssetsCurrent)
#
# > Amount of asset related to consideration paid in advance for costs that
# > provide economic benefits in future periods, and amount of other assets
# > that are expected to be realized or consumed within one year or the normal
# > operating cycle, if longer.
#
# * [Other Current Assets (OCA)]
# > (https://www.investopedia.com/terms/o/othercurrentassets.asp)
#
# > Other current assets (OCA) is a category of things of value that a company
# > owns, benefits from, or uses to generate income that can be converted into
# > cash within one business cycle. They are referred to as “other” because
# > they are uncommon or insignificant, unlike typical current asset items such
# > as cash, securities, accounts receivable, inventory, and prepaid expenses.
# --------------------------------------------------------------------------------
def get_bs_current_asset_other(soup, attributes: dict):
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:PrepaidExpenseCurrent$",
        rf"^{NAMESPACE_GAAP}:PrepaidExpenseAndOtherAssetsCurrent$",
        rf"^{NAMESPACE_GAAP}:OperatingLeaseRightOfUseAsset$",
        rf"^{NAMESPACE_GAAP}:OtherAssetsCurrent$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_current_assets(soup, attributes: dict):
    """Total Current Assets"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:AssetsCurrent$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_BS,
        rep=FS_ELEMENT_REP_CURRENT_ASSETS
    )


# --------------------------------------------------------------------------------
# BS Non Current Assets
# --------------------------------------------------------------------------------
def get_bs_non_current_asset_property_and_equipment(soup, attributes: dict):
    """Property, Plant, Equipment"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:PropertyPlantAndEquipmentNet$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_non_current_asset_restricted_cash_and_equivalent(soup, attributes: dict):
    """Restricted Cash Non Current"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:RestrictedCashAndCashEquivalentsNoncurrent$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_non_current_asset_deferred_income_tax(soup, attributes: dict):
    """Deferred Tax"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:DeferredIncomeTaxAssetsNet$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_non_current_asset_goodwill(soup, attributes: dict):
    """GoodWill"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:GoodWill$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_non_current_asset_other(soup, attributes: dict):
    """Intangible and Other Assets"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:IntangibleAssetsNetExcludingGoodwill$",
        rf"^{NAMESPACE_GAAP}:OtherAssetsNoncurrent$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_total_assets(soup, attributes: dict):
    """Total Assets"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:Assets$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_BS,
        rep=FS_ELEMENT_REP_TOTAL_ASSETS
    )


# --------------------------------------------------------------------------------
# BS Current Liabilities
# --------------------------------------------------------------------------------
def get_bs_current_liability_account_payable(soup, attributes: dict):
    """Account Payable
    * [Accounts Payable (AP)](https://www.investopedia.com/terms/a/accountspayable.asp)
    > company's obligation to pay off a short-term debt to its creditors or suppliers.
    > costs for goods and services already delivered to a company for which
    > it must pay in the future. A company can accrue liabilities for any number
    > of obligations and are recorded on the company's balance sheet.
    > They are normally listed on the balance sheet as current liabilities
    > and are adjusted at the end of an accounting period
    
    * [Accrued Liability](https://www.investopedia.com/terms/a/accrued-liability.asp)
    * [us-gaap:AccruedLiabilitiesCurrent]
    (http://xbrlsite.azurewebsites.net/2019/Prototype/references/us-gaap/Element-354.html)
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:AccountsPayableCurrent$",
        rf"^{NAMESPACE_GAAP}:AccruedLiabilitiesCurrent$",
        rf"^{NAMESPACE_GAAP}:CapitalExpendituresIncurredButNotYetPaid$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_current_liability_tax(soup, attributes: dict):
    """Tax"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:TaxesPayableCurrent$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_current_liability_longterm_debt(soup, attributes: dict):
    """Debt due to pay"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:LongTermDebtCurrent$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_current_liabilities(soup, attributes: dict):
    """Total Current Liabilities"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:LiabilitiesCurrent$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_BS,
        rep=FS_ELEMENT_REP_CURRENT_LIABILITIES
    )


# --------------------------------------------------------------------------------
# BS Non Current Liabilities
# --------------------------------------------------------------------------------
def get_bs_non_current_liability_longterm_debt(soup, attributes: dict):
    """Long Term Debt"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:LongTermDebt$",
        rf"^{NAMESPACE_GAAP}:LongTermDebtNoncurrent$",
        rf"^{NAMESPACE_GAAP}:LongTermLoansFromBank$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_non_current_liability_deferred_tax(soup, attributes: dict):
    """Tax Deferred"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:DeferredIncomeTaxLiabilitiesNet$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_non_current_liability_other(soup, attributes: dict):
    """
    Other Long Term Liabilities
    Postemployment Benefits Liability, Noncurrent
    * [us-gaap:PostemploymentBenefitsLiabilityNoncurrent]
    (http://xbrlsite.azurewebsites.net/2019/Prototype/references/us-gaap/Element-12380.html)

    > The obligations recognized for the various benefits provided to former
    > or inactive employees, their beneficiaries, and covered dependents after
    > employment but before retirement that is payable after one year 
    (or beyond the operating cycle if longer).
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:PostemploymentBenefitsLiabilityNoncurrent$",
        rf"^{NAMESPACE_GAAP}:PensionAndOtherPostretirementAndPostemploymentBenefitPlansLiabilitiesNoncurrent$",
        rf"^{NAMESPACE_GAAP}:OperatingLeaseLiabilityNoncurrent$",
        rf"^{NAMESPACE_GAAP}:OtherLiabilitiesNoncurrent$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_total_liabilities(soup, attributes: dict):
    """Total Liabilities"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:Liabilities$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_BS,
        rep=FS_ELEMENT_REP_LIABILITIES
    )


# --------------------------------------------------------------------------------
# BS Stockholders Equity
# --------------------------------------------------------------------------------
def get_bs_stockholders_equity_paid_in(soup, attributes: dict):
    """Paid-in Capital
    * [Paid-In Capital]
    (https://www.investopedia.com/terms/p/paidincapital.aspPaid-In Capital)
    > Paid-in capital represents the funds raised by the business through
    selling its equity and not from ongoing business operations.
    Paid-in capital also refers to a line item on the company's balance sheet
    listed under shareholders' equity (or stockholders' equity),
    often shown alongside the line item for additional paid-in capital.

    * [Additional Paid-In Capital (APIC)]
    (https://www.investopedia.com/terms/a/additionalpaidincapital.asp)

    > Often referred to as "contributed capital in excess of par,” APIC
    occurs when an investor buys newly-issued shares directly from a
    company during its initial public offering (IPO) stage. APIC, which is
    itemized under the shareholder equity (SE) section of a balance sheet,
    is viewed as a profit opportunity for companies as it results in them
    receiving excess cash from stockholders.
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:CommonStockValue$",
        rf"^{NAMESPACE_GAAP}:AdditionalPaidInCapitalCommonStock$",
        rf"^{NAMESPACE_GAAP}:CommonStocksIncludingAdditionalPaidInCapital$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_stockholders_equity_retained(soup, attributes: dict):
    """Retained Earnings
    * [Retained Earnings]
    (https://www.investopedia.com/terms/r/retainedearnings.asp)
    The word "retained" captures the fact that because those earnings
    were **NOT paid out to shareholders as dividends** they were instead
    retained by the company.

    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:RetainedEarningsAppropriated$",
        rf"^{NAMESPACE_GAAP}:RetainedEarningsAccumulatedDeficit$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes
    )


def get_bs_stockholders_equity_other(soup, attributes: dict):
    """Accumulated other comprehensive income/loss"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:AccumulatedOtherComprehensiveIncomeLossNetOfTax$",
        rf"^{NAMESPACE_GAAP}:MinorityInterest$",
    ]), re.IGNORECASE)
    return get_records_for_financial_element_names(
        soup=soup, names=names, attributes=attributes)


def get_bs_stockholders_equity(soup, attributes: dict):
    """Stockholder's Equity
    * [Stockholders' Equity]
    (https://www.investopedia.com/terms/s/stockholdersequity.asp)
    Remaining amount of assets available to shareholders after all
    liabilities have been paid. (純資産)
    """
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:StockholdersEquity$",
        rf"^{NAMESPACE_GAAP}:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_BS,
        rep=FS_ELEMENT_REP_EQUITY
    )


def get_bs_total_liabilities_and_stockholders_equity(soup, attributes: dict):
    """Total Liabilities + Stockholder's Equity"""
    names = re.compile("|".join([
        rf"^{NAMESPACE_GAAP}:LiabilitiesAndStockholdersEquity$",
    ]), re.IGNORECASE)
    return represents(
        get_records_for_financial_element_names(
            soup=soup, names=names, attributes=attributes
        ),
        fs=FS_BS,
        rep=FS_ELEMENT_REP_EQUITY_AND_LIABILITIES
    )
