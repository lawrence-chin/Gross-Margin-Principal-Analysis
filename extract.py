from functional_functions import get_snowflake_connection, query_snowflake
from datetime import datetime as dt

def get_acct_transaction_data(begin_date, end_date):
        
    print('Running now ' + str(dt.now()))
    full_query = query_snowflake("""select tb1.Market_Region,
    tb1.Side_Represented,
    tb1.Compass_Deals_Id,
    tb1.Listing_Type,
    Location_Name,
    Department_Name,
    tb1.Compass_Team_ID,
    tb1.Close_Date,
    tb1.Ending_Period,
    tb1.Final_Close_Price,
    tb1.Total_GCI,
    (sum(tb1.total_referral) + sum(tb1.Total_GCI)) as GCI_Less_Referral,
    tb1.commission_expense,
    (sum(tb1.total_referral) + sum(tb1.Total_GCI) + sum(tb1.commission_expense)) as Company_Dollar,
    tb1.AEP,
    (sum(tb1.total_referral) + sum(tb1.Total_GCI) + sum(tb1.commission_expense)) + sum(tb1.AEP) as Company_Dollar_Plus_AEP,
    tb1.Gross_Commission_Revenue__Condo_coop_Sales_400001,
    tb1.Gross_Commission_Revenue__House_Single_Family_400002,
    tb1.Gross_Commission_Revenue__Multi_Family_400003,
    tb1.Gross_Commission_Revenue__Townhouse_Single_Residence_400004,
    tb1.Gross_Commission_Revenue__Residential_Rentals_400005,
    tb1.Gross_Commission_Revenue__Commercial_Sale_400006,
    tb1.Gross_Commission_Revenue__Commercial_Lease_400007,
    tb1.Gross_Commission_Revenue__Other_Property_GCI_400009,
    tb1.Gross_Commission_Revenue__Credits_Discounts_from_Commission_401001,
    tb1.Referral_Revenue__External_Referral_Revenue_410001,
    tb1.Referral_Revenue__Internal_Referral_Revenue_411001,
    tb1.Ancillary_Revenue__Admin__Resource_Fees_430003,
    tb1.Commissions_and_other_transaction_related_costs_COGS__Commission_Expense_502001,
    tb1.Commissions_and_other_transaction_related_costs_COGS__Commission_Expense__Agent_Equity_Program_502006,
    tb1.Commissions_and_other_transaction_related_costs_COGS__ProductionBased_Bonus_503001,
    tb1.Referral_Expense_COGS__Internal_Referral_Expense_510002,
    tb1.Referral_Expense_COGS__Referral_Expense___Compass_Agent_Compass_Deal_510003,
    tb1.Referral_Expense_COGS__Referral_Expense__External_Brokerage_510004,
    tb1.Referral_Expense_COGS__Internal_Referral__Subsidiaries_510005,
    tb1.Referral_Expense_COGS__Referral_Expense__Compass_Agent_External_Deal_510006,
    tb1.Gross_Commission_Revenue__Gross_Commission_Income__All_Other_400090,
    tb1.Gross_Commission_Revenue__Commission_RevenueNYC_400097,
    tb1.Gross_Commission_Revenue__Commission_RevenueDC_400098,
    tb1.Gross_Commission_Revenue__Commission_RevenueMIA_400099,
    tb1.Gross_Commission_Revenue__RefundsAllowances_401002,
    tb1.Gross_Commission_Revenue__Client_Pass_Through_401003,
    tb1.Gross_Commission_Revenue__Renewals_401004,
    Ancillary_Revenue__Ancillary_Revenue_430001,
    Ancillary_Revenue__E_and_O_Billing_Income_430006,
    Ancillary_Revenue__Discounted_Vendor_Service_Program_430009
    From
    (select CASE WHEN a.Market_Region='Texas' THEN SPLIT_PART(a.Location_Name,',',0) ELSE a.Market_Region END as Market_Region,
    a.Side_Represented,
    CAST(CAST(COALESCE(a.Compass_Deals_Id,a.Compass_Deal_Id_Line_Id) as INTEGER) as STRING) as Compass_Deals_Id,
    a.Listing_Type,
    a.Location_Name,
    a.Department_Name,
    a.Compass_Team_ID,
    a.Close_Date,
    a.Ending_Period,
    a.Final_Close_Price,
    sum(case when a.account_number in('400001',
                                        '400002',
                                        '400003',
                                        '400004',
                                        '400005',
                                        '400006',
                                        '400007', 
                                        '400009',
                                        '400090',
                                        '400097',
                                        '400098',
                                        '400099',
                                        '401001',
                                        '401002',
                                        '401003',
                                        '401004',
                                        '410001',
                                        '411001')then a.amount else 0 End) as Total_GCI,
    sum(case when a.account_number in ('510004','510002','510005') then a.amount else 0 end) as total_referral,
    sum(case when a.account_number in ('502001','510003','510006') then a.amount else 0 end) as commission_expense,
    sum(case when a.account_number = '502006' then a.amount else 0 end) as AEP,
    Sum(Case When  a.account_number = '400001' Then a.amount Else 0 End) as Gross_Commission_Revenue__Condo_coop_Sales_400001,
    Sum(Case When  a.account_number = '400002' Then a.amount Else 0 End) as Gross_Commission_Revenue__House_Single_Family_400002,
    Sum(Case When  a.account_number = '400003' Then a.amount Else 0 End) as Gross_Commission_Revenue__Multi_Family_400003,
    Sum(Case When  a.account_number = '400004' Then a.amount Else 0 End) as Gross_Commission_Revenue__Townhouse_Single_Residence_400004,
    Sum(Case When  a.account_number = '400005' Then a.amount Else 0 End) as Gross_Commission_Revenue__Residential_Rentals_400005,
    Sum(Case When  a.account_number = '400006' Then a.amount Else 0 End) as Gross_Commission_Revenue__Commercial_Sale_400006,
    Sum(Case When  a.account_number = '400007' Then a.amount Else 0 End) as Gross_Commission_Revenue__Commercial_Lease_400007,
    Sum(Case When  a.account_number = '400009' Then a.amount Else 0 End) as Gross_Commission_Revenue__Other_Property_GCI_400009,
    Sum(Case When  a.account_number = '401001' Then a.amount Else 0 End) as Gross_Commission_Revenue__Credits_Discounts_from_Commission_401001,
    Sum(Case When  a.account_number = '410001' Then a.amount Else 0 End) as Referral_Revenue__External_Referral_Revenue_410001,
    Sum(Case When  a.account_number = '411001' Then a.amount Else 0 End) as Referral_Revenue__Internal_Referral_Revenue_411001,
    Sum(Case When  a.account_number = '502001' Then a.amount Else 0 End) as Commissions_and_other_transaction_related_costs_COGS__Commission_Expense_502001,
    Sum(Case When  a.account_number = '502006' Then a.amount Else 0 End) as Commissions_and_other_transaction_related_costs_COGS__Commission_Expense__Agent_Equity_Program_502006,
    Sum(Case When  a.account_number = '503001' Then a.amount Else 0 End) as Commissions_and_other_transaction_related_costs_COGS__ProductionBased_Bonus_503001,
    Sum(Case When  a.account_number = '510002' Then a.amount Else 0 End) as Referral_Expense_COGS__Internal_Referral_Expense_510002,
    Sum(Case When  a.account_number = '510003' Then a.amount Else 0 End) as Referral_Expense_COGS__Referral_Expense___Compass_Agent_Compass_Deal_510003,
    Sum(Case When  a.account_number = '510004' Then a.amount Else 0 End) as Referral_Expense_COGS__Referral_Expense__External_Brokerage_510004,
    Sum(Case When  a.account_number = '510005' Then a.amount Else 0 End) as Referral_Expense_COGS__Internal_Referral__Subsidiaries_510005,
    Sum(Case When  a.account_number = '510006' Then a.amount Else 0 End) as Referral_Expense_COGS__Referral_Expense__Compass_Agent_External_Deal_510006,
    Sum(Case When  a.account_number = '400090' Then a.amount Else 0 End) as Gross_Commission_Revenue__Gross_Commission_Income__All_Other_400090,
    Sum(Case When  a.account_number = '400097' Then a.amount Else 0 End) as Gross_Commission_Revenue__Commission_RevenueNYC_400097,
    Sum(Case When  a.account_number = '400098' Then a.amount Else 0 End) as Gross_Commission_Revenue__Commission_RevenueDC_400098,
    Sum(Case When  a.account_number = '400099' Then a.amount Else 0 End) as Gross_Commission_Revenue__Commission_RevenueMIA_400099,
    Sum(Case When  a.account_number = '401002' Then a.amount Else 0 End) as Gross_Commission_Revenue__RefundsAllowances_401002,
    Sum(Case When  a.account_number = '401003' Then a.amount Else 0 End) as Gross_Commission_Revenue__Client_Pass_Through_401003,
    Sum(Case When  a.account_number = '401004' Then a.amount Else 0 End) as Gross_Commission_Revenue__Renewals_401004,
    Sum(Case When  a.account_number = '430001' Then a.amount Else 0 End) as Ancillary_Revenue__Ancillary_Revenue_430001,
    Sum(Case When  a.account_number = '430003' Then a.amount Else 0 End) as Ancillary_Revenue__Admin__Resource_Fees_430003,
    Sum(Case When  a.account_number = '430006' Then a.amount Else 0 End) as Ancillary_Revenue__E_and_O_Billing_Income_430006,
    Sum(Case When  a.account_number = '430009' Then a.amount Else 0 End) as Ancillary_Revenue__Discounted_Vendor_Service_Program_430009
    from "PC_STITCH_DB"."NETSUITE_ANALYTICS"."VIEW_ACCOUNTING_TRANSACTION_LINES" a 
    Where 
        a.Ending_Period between to_Date('{b}','mm-dd-yyyy') AND to_Date('{e}','mm-dd-yyyy')
    group by 1,
    a.Compass_Team_ID,
    3,
    a.Side_Represented,
    a.Listing_Type,
    a.Location_Name,
    a.Department_Name,
    a.Ending_Period,
    a.Close_Date,
    a.Final_Close_Price) tb1
    Group by tb1.Market_Region,
    tb1.Side_Represented,
    tb1.Compass_Deals_Id,
    tb1.Listing_Type,
    Location_Name,
    Department_Name,
    tb1.Compass_Team_ID,
    tb1.Close_Date,
    tb1.Ending_Period,
    tb1.Final_Close_Price,
    tb1.Total_GCI,
    tb1.commission_expense,
    tb1.AEP,
    tb1.Gross_Commission_Revenue__Condo_coop_Sales_400001,
    tb1.Gross_Commission_Revenue__House_Single_Family_400002,
    tb1.Gross_Commission_Revenue__Multi_Family_400003,
    tb1.Gross_Commission_Revenue__Townhouse_Single_Residence_400004,
    tb1.Gross_Commission_Revenue__Residential_Rentals_400005,
    tb1.Gross_Commission_Revenue__Commercial_Sale_400006,
    tb1.Gross_Commission_Revenue__Commercial_Lease_400007,
    tb1.Gross_Commission_Revenue__Other_Property_GCI_400009,
    tb1.Gross_Commission_Revenue__Credits_Discounts_from_Commission_401001,
    tb1.Referral_Revenue__External_Referral_Revenue_410001,
    tb1.Referral_Revenue__Internal_Referral_Revenue_411001,
    tb1.Ancillary_Revenue__Admin__Resource_Fees_430003,
    tb1.Commissions_and_other_transaction_related_costs_COGS__Commission_Expense_502001,
    tb1.Commissions_and_other_transaction_related_costs_COGS__Commission_Expense__Agent_Equity_Program_502006,
    tb1.Commissions_and_other_transaction_related_costs_COGS__ProductionBased_Bonus_503001,
    tb1.Referral_Expense_COGS__Internal_Referral_Expense_510002,
    tb1.Referral_Expense_COGS__Referral_Expense___Compass_Agent_Compass_Deal_510003,
    tb1.Referral_Expense_COGS__Referral_Expense__External_Brokerage_510004,
    tb1.Referral_Expense_COGS__Internal_Referral__Subsidiaries_510005,
    tb1.Referral_Expense_COGS__Referral_Expense__Compass_Agent_External_Deal_510006,
    tb1.Gross_Commission_Revenue__Gross_Commission_Income__All_Other_400090,
    tb1.Gross_Commission_Revenue__Commission_RevenueNYC_400097,
    tb1.Gross_Commission_Revenue__Commission_RevenueDC_400098,
    tb1.Gross_Commission_Revenue__Commission_RevenueMIA_400099,
    tb1.Gross_Commission_Revenue__RefundsAllowances_401002,
    tb1.Gross_Commission_Revenue__Client_Pass_Through_401003,
    tb1.Gross_Commission_Revenue__Renewals_401004,
    Ancillary_Revenue__Ancillary_Revenue_430001,
    Ancillary_Revenue__E_and_O_Billing_Income_430006,
    Ancillary_Revenue__Discounted_Vendor_Service_Program_430009;""".format(b=begin_date,e=end_date))
    print('finish...' + str(dt.now()))
    
    return full_query


def get_aep_data():

   

    print('Running now ' + str(dt.now()))
    all_df = query_snowflake("""select
        COALESCE(A.COMPASS_DEALS_ID, A.COMPASS_DEAL_ID_LINE_ID) as COMPASS_DEALS_ID,
        A.COMPASS_TEAM_ID,
        A.ENDING_PERIOD,
        A.AMOUNT,
        -- This was done to separate out Texas into submarket CASE WHEN A.MARKET_REGION='Texas' THEN SPLIT_PART(a.Location_Name,',',0) ELSE A.MARKET_REGION END as MARKET_REGION,
        A.MARKET_REGION,
        A.SIDE_REPRESENTED,
        A.LISTING_TYPE
    from "PC_STITCH_DB"."NETSUITE_ANALYTICS"."VIEW_ACCOUNTING_TRANSACTION_LINES" A
    WHERE
        A.ENDING_PERIOD BETWEEN '2019-01-01' AND '2020-08-31'
        AND A.COMPASS_ITEM_TYPE_NAME='AEP'
        AND A.ACCOUNT_NUMBER=502001;""")
    print('finish...' + str(dt.now()))

    return all_df

def get_ica_data():
    
   

    print('Running now ' + str(dt.now()))
    ica_details = query_snowflake("""
    SELECT 
        "Compass Agent ID",
        "Compass Team ID",
        "Account Team Name",
        "Parent Account Name",
        "Agent Name",
        "Agent Role"
        "Sales Manager Name",
        "Market",
        "SubMarket",
        "Office",
        "Contact Status",
        "Original Start Date",
        "Number of Agents",
        "Current Contract Start Date",
        "Current Contract End Date",
        "ICA Expiration Date",
        "Separation Date",
        "Separated From Compass",
        "Opportunity GCI"
    FROM
    "PC_STITCH_DB"."ET_DEVELOPMENT"."VIEW_AGENT_ICA_HISTORY"
    WHERE 
        "Agent Role" = 'Principal' 
        AND "Primary Principal Flag" = 'Y'
        AND "Original Start Date" IS NOT NULL
        AND "Parent Account Name" NOT IN ('Pacific Union','Pacific Union International','Paragon','Alain Pinel Realtors')
    """)
    print('finish...' + str(dt.now()))

    return ica_details


def get_extracts(begin_date, end_date):
    transaction_data = get_acct_transaction_data(begin_date, end_date)
    aep_data = get_aep_data()
    ica_data = get_ica_data()

    return {
        'transaction_data':transaction_data,
        'aep_data':aep_data,
        'ica_data': ica_data
    }
    