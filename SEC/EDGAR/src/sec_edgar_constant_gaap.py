NAMESPACE = "us-gaap"
GAAP_CREDIT_ITEMS = set(map(str.lower, [
    # P/L
    f"{NAMESPACE}:Revenues",
    f"{NAMESPACE}:RevenueFromContractWithCustomerExcludingAssessedTax",
    f"{NAMESPACE}:DeferredRevenue",
    f"{NAMESPACE}:RevenueMineralSales",
    f"{NAMESPACE}:DeferredRevenueCurrent",
    f"{NAMESPACE}:HealthCareOrganizationRevenue",
    f"{NAMESPACE}:SalesRevenueGoodsGross",
    f"{NAMESPACE}:RealEstateRevenueNet",
    f"{NAMESPACE}:SalesOfRealEstate",
    f"{NAMESPACE}:AdvertisingRevenue",
    f"{NAMESPACE}:ElectricalGenerationRevenue",
    f"{NAMESPACE}:ContractsRevenue",
    f"{NAMESPACE}:AdmissionsRevenue",
    f"{NAMESPACE}:InsuranceServicesRevenue",
    f"{NAMESPACE}:BrokerageCommissionsRevenue",
    f"{NAMESPACE}:OtherNonoperatingIncomeExpense",  # Why this is in credit?
    f"{NAMESPACE}:CashAndCashEquivalentsAtCarryingValue",
    f"{NAMESPACE}:OtherAlternativeEnergySalesRevenue",
    f"{NAMESPACE}:RegulatedAndUnregulatedOperatingRevenue",
    
    # B/S (Assets)
    f"{NAMESPACE}:RestrictedCashAndCashEquivalentsNoncurrent",
    f"{NAMESPACE}:RestrictedCashEquivalentsCurrent",
    f"{NAMESPACE}:RestrictedCashAndCashEquivalentsAtCarryingValue",
    f"{NAMESPACE}:ShortTermInvestments",
    f"{NAMESPACE}:ReceivablesNetCurrent",
    f"{NAMESPACE}:OtherReceivables",
    f"{NAMESPACE}:AccountsReceivableNetCurrent",
    f"{NAMESPACE}:InventoryNet",
    f"{NAMESPACE}:PrepaidExpenseCurrent",
    f"{NAMESPACE}:PrepaidExpenseAndOtherAssetsCurrent",
    f"{NAMESPACE}:PropertyPlantAndEquipmentNet",
    f"{NAMESPACE}:DeferredIncomeTaxAssetsNet",
    f"{NAMESPACE}:GoodWill",
    f"{NAMESPACE}:IntangibleAssetsNetExcludingGoodwill",
    f"{NAMESPACE}:OtherAssetsCurrent",
    f"{NAMESPACE}:OtherAssetsNoncurrent",
    f"{NAMESPACE}:OperatingLeaseRightOfUseAsset",
    
    
]))

GAAP_DEBIT_ITEMS = set(map(str.lower, [
    # P/L
    f"{NAMESPACE}:CostOfRevenue",
    f"{NAMESPACE}:CostOfGoods",
    f"{NAMESPACE}:CostOfGoodsAndServicesSold",
    f"{NAMESPACE}:CostOfServicesLicensesAndServices",
    f"{NAMESPACE}:ContractRevenueCost",
    f"{NAMESPACE}:LicenseCosts",
    f"{NAMESPACE}:ResearchAndDevelopmentExpense",
    f"{NAMESPACE}:SellingGeneralAndAdministrativeExpense",
    f"{NAMESPACE}:OtherCostAndExpenseOperating",
    f"{NAMESPACE}:IncomeTaxExpenseBenefit",
    f"{NAMESPACE}:InterestExpense",
    
    # B/S (Liabilities)
    f"{NAMESPACE}:AccountsPayableCurrent",
    f"{NAMESPACE}:CapitalExpendituresIncurredButNotYetPaid",
    f"{NAMESPACE}:AccruedLiabilitiesCurrent",
    f"{NAMESPACE}:TaxesPayableCurrent",
    f"{NAMESPACE}:LongTermDebtCurrent",
    f"{NAMESPACE}:LongTermLoansFromBank",
    f"{NAMESPACE}:DeferredIncomeTaxLiabilitiesNet",
    f"{NAMESPACE}:LongTermDebt",
    f"{NAMESPACE}:LongTermDebtNoncurrent",
    f"{NAMESPACE}:PostemploymentBenefitsLiabilityNoncurrent",
    f"{NAMESPACE}:PensionAndOtherPostretirementAndPostemploymentBenefitPlansLiabilitiesNoncurrent",
    f"{NAMESPACE}:OperatingLeaseLiabilityNoncurrent",
    f"{NAMESPACE}:OtherLiabilitiesNoncurrent",
    # B/S (SE)
    f"{NAMESPACE}:AdditionalPaidInCapitalCommonStock",
    f"{NAMESPACE}:CommonStockValue",
    f"{NAMESPACE}:CommonStocksIncludingAdditionalPaidInCapital",
    f"{NAMESPACE}:RetainedEarningsAppropriated",
    f"{NAMESPACE}:RetainedEarningsAccumulatedDeficit",
    f"{NAMESPACE}:AccumulatedOtherComprehensiveIncomeLossNetOfTax",
    f"{NAMESPACE}:MinorityInterest",
    
]))

GAAP_CALC_ITEMS = set(map(str.lower, [
    # P/L
    f"{NAMESPACE}:OperatingExpenses",
    f"{NAMESPACE}:GrossProfit",
    f"{NAMESPACE}:OperatingIncomeLoss",
    f"{NAMESPACE}:ProfitLoss",    
    f"{NAMESPACE}:NetIncomeLoss",    
    f"{NAMESPACE}:NonoperatingIncomeExpense",

    # B/S (Credit)
    f"{NAMESPACE}:AssetsCurrent",    
    f"{NAMESPACE}:Assets",    
    # B/S (Debit/Liabilities)
    f"{NAMESPACE}:LiabilitiesCurrent",
    f"{NAMESPACE}:Liabilities",
    # B/S (Debit/SE)
    f"{NAMESPACE}:StockholdersEquity",
    f"{NAMESPACE}:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    f"{NAMESPACE}:LiabilitiesAndStockholdersEquity",
    
]))

GAAP_METRIC_ITEMS = set(map(str.lower, [
    f"{NAMESPACE}:EarningsPerShareBasic",
    f"{NAMESPACE}:EarningsPerShareBasicAndDiluted",
]))

GAAP_FACT_ITEMS = set(map(str.lower, [
    f"{NAMESPACE}:SharesOutstanding",
    f"{NAMESPACE}:CommonStockSharesOutstanding",
    f"{NAMESPACE}:CommonStockOtherSharesOutstanding",
]))

