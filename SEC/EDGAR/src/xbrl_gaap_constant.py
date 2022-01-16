NAMESPACE_GAAP = "us-gaap"
GAAP_CREDIT_ITEMS = set(map(str.lower, [
    # P/L
    f"{NAMESPACE_GAAP}:Revenues",
    f"{NAMESPACE_GAAP}:SalesRevenueNet",
    f"{NAMESPACE_GAAP}:SalesRevenueGoodsNet",
    f"{NAMESPACE_GAAP}:DeferredRevenue",
    f"{NAMESPACE_GAAP}:RevenueMineralSales",
    f"{NAMESPACE_GAAP}:DeferredRevenueCurrent",
    f"{NAMESPACE_GAAP}:HealthCareOrganizationRevenue",
    f"{NAMESPACE_GAAP}:SalesRevenueGoodsGross",
    f"{NAMESPACE_GAAP}:SalesRevenueServicesNet",
    f"{NAMESPACE_GAAP}:RealEstateRevenueNet",
    f"{NAMESPACE_GAAP}:SalesOfRealEstate",
    f"{NAMESPACE_GAAP}:AdvertisingRevenue",
    f"{NAMESPACE_GAAP}:ElectricalGenerationRevenue",
    f"{NAMESPACE_GAAP}:ContractsRevenue",
    f"{NAMESPACE_GAAP}:AdmissionsRevenue",
    f"{NAMESPACE_GAAP}:InsuranceServicesRevenue",
    f"{NAMESPACE_GAAP}:BrokerageCommissionsRevenue",
    f"{NAMESPACE_GAAP}:OtherNonoperatingIncomeExpense",  # Why this is in credit?
    f"{NAMESPACE_GAAP}:CashAndCashEquivalentsAtCarryingValue",
    f"{NAMESPACE_GAAP}:RevenueFromContractWithCustomerExcludingAssessedTax",
    f"{NAMESPACE_GAAP}:OtherAlternativeEnergySalesRevenue",
    f"{NAMESPACE_GAAP}:RegulatedAndUnregulatedOperatingRevenue",
    f"{NAMESPACE_GAAP}:OperatingLeasesIncomeStatementLeaseRevenue",
    f"{NAMESPACE_GAAP}:OtherNonoperatingIncome",
    f"{NAMESPACE_GAAP}:InvestmentIncomeInterest",
    f"{NAMESPACE_GAAP}:InterestPaid",
    f"{NAMESPACE_GAAP}:InterestAndOtherIncome",
    f"{NAMESPACE_GAAP}:InterestIncomeOther",
    f"{NAMESPACE_GAAP}:InsuranceCommissionsAndFees",
    f"{NAMESPACE_GAAP}:DerivativeGainOnDerivative",
    f"{NAMESPACE_GAAP}:InterestAndDividendIncomeOperating",
    f"{NAMESPACE_GAAP}:InvestmentIncomeInterestAndDividend",
    f"{NAMESPACE_GAAP}:InterestAndFeeIncomeLoansConsumerInstallmentAutomobilesMarineAndOtherVehicles",
    f"{NAMESPACE_GAAP}:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",

    # B/S (Assets)
    f"{NAMESPACE_GAAP}:RestrictedCashAndCashEquivalentsNoncurrent",
    f"{NAMESPACE_GAAP}:RestrictedCashEquivalentsCurrent",
    f"{NAMESPACE_GAAP}:RestrictedCashAndCashEquivalentsAtCarryingValue",
    f"{NAMESPACE_GAAP}:ShortTermInvestments",
    f"{NAMESPACE_GAAP}:ReceivablesNetCurrent",
    f"{NAMESPACE_GAAP}:OtherReceivables",
    f"{NAMESPACE_GAAP}:AccountsReceivableNetCurrent",
    f"{NAMESPACE_GAAP}:InventoryNet",
    f"{NAMESPACE_GAAP}:PrepaidExpenseCurrent",
    f"{NAMESPACE_GAAP}:PrepaidExpenseAndOtherAssetsCurrent",
    f"{NAMESPACE_GAAP}:PropertyPlantAndEquipmentNet",
    f"{NAMESPACE_GAAP}:DeferredIncomeTaxAssetsNet",
    f"{NAMESPACE_GAAP}:GoodWill",
    f"{NAMESPACE_GAAP}:IntangibleAssetsNetExcludingGoodwill",
    f"{NAMESPACE_GAAP}:OtherAssetsCurrent",
    f"{NAMESPACE_GAAP}:OtherAssetsNoncurrent",
    f"{NAMESPACE_GAAP}:OperatingLeaseRightOfUseAsset",
    f"{NAMESPACE_GAAP}:Depreciation",
    f"{NAMESPACE_GAAP}:DepreciationAndAmortization",
    f"{NAMESPACE_GAAP}:DepreciationDepletionAndAmortization",
]))

GAAP_DEBIT_ITEMS = set(map(str.lower, [
    # P/L
    f"{NAMESPACE_GAAP}:CostOfRevenue",
    f"{NAMESPACE_GAAP}:CostOfGoods",
    f"{NAMESPACE_GAAP}:CostOfGoodsAndServicesSold",
    f"{NAMESPACE_GAAP}:CostOfServicesLicensesAndServices",
    f"{NAMESPACE_GAAP}:ContractRevenueCost",
    f"{NAMESPACE_GAAP}:LicenseCosts",
    f"{NAMESPACE_GAAP}:ResearchAndDevelopmentExpense",
    f"{NAMESPACE_GAAP}:SellingGeneralAndAdministrativeExpense",
    f"{NAMESPACE_GAAP}:OtherCostAndExpenseOperating",
    f"{NAMESPACE_GAAP}:IncomeTaxExpenseBenefit",
    f"{NAMESPACE_GAAP}:InterestExpense",
    f"{NAMESPACE_GAAP}:MarketingExpense",
    f"{NAMESPACE_GAAP}:EquipmentExpense",
    f"{NAMESPACE_GAAP}:OtherExpenses",
    f"{NAMESPACE_GAAP}:LaborAndRelatedExpense",
    f"{NAMESPACE_GAAP}:ProvisionForLoanLeaseAndOtherLosses",
    f"{NAMESPACE_GAAP}:DividendsCash",
    f"{NAMESPACE_GAAP}:PaymentsOfDividendsCommonStock",
    f"{NAMESPACE_GAAP}:DistributedEarnings",

    # B/S (Liabilities)
    f"{NAMESPACE_GAAP}:AccountsPayableCurrent",
    f"{NAMESPACE_GAAP}:CapitalExpendituresIncurredButNotYetPaid",
    f"{NAMESPACE_GAAP}:AccruedLiabilitiesCurrent",
    f"{NAMESPACE_GAAP}:TaxesPayableCurrent",
    f"{NAMESPACE_GAAP}:LongTermDebtCurrent",
    f"{NAMESPACE_GAAP}:LongTermLoansFromBank",
    f"{NAMESPACE_GAAP}:DeferredIncomeTaxLiabilitiesNet",
    f"{NAMESPACE_GAAP}:LongTermDebt",
    f"{NAMESPACE_GAAP}:LongTermDebtNoncurrent",
    f"{NAMESPACE_GAAP}:PostemploymentBenefitsLiabilityNoncurrent",
    f"{NAMESPACE_GAAP}:PensionAndOtherPostretirementAndPostemploymentBenefitPlansLiabilitiesNoncurrent",
    f"{NAMESPACE_GAAP}:OperatingLeaseLiabilityNoncurrent",
    f"{NAMESPACE_GAAP}:OtherLiabilitiesNoncurrent",
    # B/S (SE)
    f"{NAMESPACE_GAAP}:AdditionalPaidInCapitalCommonStock",
    f"{NAMESPACE_GAAP}:CommonStockValue",
    f"{NAMESPACE_GAAP}:CommonStocksIncludingAdditionalPaidInCapital",
    f"{NAMESPACE_GAAP}:RetainedEarningsAppropriated",
    f"{NAMESPACE_GAAP}:RetainedEarningsAccumulatedDeficit",
    f"{NAMESPACE_GAAP}:AccumulatedOtherComprehensiveIncomeLossNetOfTax",
    f"{NAMESPACE_GAAP}:MinorityInterest",

]))

GAAP_CALC_ITEMS = set(map(str.lower, [
    # P/L
    f"{NAMESPACE_GAAP}:OperatingExpenses",
    f"{NAMESPACE_GAAP}:GrossProfit",
    f"{NAMESPACE_GAAP}:OperatingIncomeLoss",
    f"{NAMESPACE_GAAP}:ProfitLoss",
    f"{NAMESPACE_GAAP}:NetIncomeLoss",
    f"{NAMESPACE_GAAP}:NonoperatingIncomeExpense",

    # B/S (Credit)
    f"{NAMESPACE_GAAP}:AssetsCurrent",
    f"{NAMESPACE_GAAP}:Assets",
    # B/S (Debit/Liabilities)
    f"{NAMESPACE_GAAP}:LiabilitiesCurrent",
    f"{NAMESPACE_GAAP}:Liabilities",
    # B/S (Debit/SE)
    f"{NAMESPACE_GAAP}:StockholdersEquity",
    f"{NAMESPACE_GAAP}:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    f"{NAMESPACE_GAAP}:LiabilitiesAndStockholdersEquity",

]))

GAAP_METRIC_ITEMS = set(map(str.lower, [
    f"{NAMESPACE_GAAP}:EarningsPerShareBasic",
    f"{NAMESPACE_GAAP}:EarningsPerShareBasicAndDiluted",
]))

GAAP_FACT_ITEMS = set(map(str.lower, [
    f"{NAMESPACE_GAAP}:SharesOutstanding",
    f"{NAMESPACE_GAAP}:CommonStockSharesOutstanding",
    f"{NAMESPACE_GAAP}:CommonStockOtherSharesOutstanding",
]))
