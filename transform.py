import params
import pandas as pd
import numpy as np

def get_transforms(extract_dict):
    acct_data = extract_dict['transaction_data'].copy()
    aep_data = extract_dict['aep_data'].copy()
    ica_data = extract_dict['ica_data'].copy()
    af_data = extract_dict['af_data'].copy()
    market_crosswalk = params.market_crosswalk
    cohort_mix_dict = params.cohort_mix_dict

    #Raw data adjustemnts
    ica_data.loc[ica_data['Number of Agents']==0, 'Number of Agents'] = 1
    acct_data['LOCATION_NAME'].fillna("tmp",inplace=True)

    #Admin Fees Data adjustments
    af_data = af_data.apply(lambda x: x.fillna(0) if x.dtype.kind in 'biufc' else x.fillna('tmp'))

    cols = af_data.columns.tolist()
    cols.remove('AMOUNT')
    cols.remove('ITEM_ID')
    cols.remove('ITEM_NAME')

    #SUMS with account number for adjustments
    afdf_sums = af_data.groupby(cols)['AMOUNT'].sum().reset_index()
    afdf_sums.rename(columns={'AMOUNT':'FLAT_FEES_ISOLATED'},inplace=True)

    #SUMS to combine individual account numbers for total iso fee
    cols.remove('ACCOUNT_NUMBER')
    afdf_totals = afdf_sums.groupby(cols)['FLAT_FEES_ISOLATED'].sum().reset_index()

    accts_for_af = afdf_sums['ACCOUNT_NUMBER'].unique().tolist()

    #Have to leave as tmp to account for groupby and join on nulls
    aep_data.fillna('tmp',inplace=True)

    copy_df = aep_data[aep_data['COMPASS_DEALS_ID']!='tmp'].copy() 

    output1 = copy_df.groupby(['COMPASS_DEALS_ID','COMPASS_TEAM_ID','ENDING_PERIOD','MARKET_REGION','SIDE_REPRESENTED','LISTING_TYPE'])['AMOUNT'].sum().reset_index()
    output1['COMPASS_DEALS_ID'] = output1['COMPASS_DEALS_ID'].astype(int).astype(str)

    nulls_only = aep_data[aep_data['COMPASS_DEALS_ID']=='tmp'].copy()
    nulls_only = nulls_only.fillna('tmp').groupby(['COMPASS_DEALS_ID','COMPASS_TEAM_ID','ENDING_PERIOD','MARKET_REGION','SIDE_REPRESENTED','LISTING_TYPE'])['AMOUNT'].sum().reset_index()

    final = pd.concat([output1, nulls_only])

    #Account for NULLs in joins
    acct_data[['COMPASS_DEALS_ID','COMPASS_TEAM_ID','MARKET_REGION','SIDE_REPRESENTED','LISTING_TYPE']] = \
    acct_data[['COMPASS_DEALS_ID','COMPASS_TEAM_ID','MARKET_REGION','SIDE_REPRESENTED','LISTING_TYPE']].fillna('tmp')

    all_w_aep = acct_data.merge(final.rename(columns={'AMOUNT':'AEP_ADJUSTED'}), on=['COMPASS_DEALS_ID','COMPASS_TEAM_ID','ENDING_PERIOD','MARKET_REGION','SIDE_REPRESENTED','LISTING_TYPE'], how='outer')
    all_w_aep.loc[all_w_aep['AEP_ADJUSTED'].notnull(), 'COMMISSION_EXPENSE'] = all_w_aep['COMMISSION_EXPENSE'] - all_w_aep['AEP_ADJUSTED']

    all_w_aep_af = all_w_aep.merge(afdf_sums, on=['COMPASS_DEALS_ID','COMPASS_TEAM_ID','ENDING_PERIOD','MARKET_REGION','LOCATION_NAME','SIDE_REPRESENTED','LISTING_TYPE','DEPARTMENT_NAME'], how='left')

    all_w_aep_af_cols = all_w_aep_af.columns.tolist()

    for each in accts_for_af:
        col_to_adjust = [i for i in all_w_aep_af_cols if each in i]
        if not col_to_adjust:
            print('Cannot find accompany account number column for: ' + str(each))
        else:
            col_to_adjust = col_to_adjust[0]
            all_w_aep_af.loc[(all_w_aep_af['ACCOUNT_NUMBER']==each) & (all_w_aep_af['FLAT_FEES_ISOLATED'].notnull()), col_to_adjust] = all_w_aep_af[col_to_adjust] - all_w_aep_af['FLAT_FEES_ISOLATED']

    all_w_aep_af.drop(columns={'ACCOUNT_NUMBER','FLAT_FEES_ISOLATED'},axis=1,inplace=True)

    all_w_aep_af = all_w_aep_af.merge(afdf_totals, on=['COMPASS_DEALS_ID','COMPASS_TEAM_ID','ENDING_PERIOD','MARKET_REGION','LOCATION_NAME','SIDE_REPRESENTED','LISTING_TYPE','DEPARTMENT_NAME'], how='outer')

    all_w_aep_af['ANCILLARY_REVENUE__FLAT_FEES_430011'] = all_w_aep_af['ANCILLARY_REVENUE__FLAT_FEES_430011'] + all_w_aep_af['FLAT_FEES_ISOLATED']

    #Join in ICA data
    ica_redux = ica_data[ica_data['Compass Team ID'].notnull()][['Compass Team ID',
                                                                   'Account Team Name',
                                                                   'Original Start Date',
                                                                   'Market',
                                                                   'Separation Date',
                                                                   'Number of Agents',
                                                                   'Total Split',
                                                                   'Base Split',
                                                                   	"Resource Fee Type", 
                                                                    "Resource Fee Option" , 
                                                                    "Resource Fee %",
                                                                    "Resource Fee Cap",
                                                                    "Actual Resource Fee % or #",
                                                                    "Policy Resource Fee % or #"]].copy()

    detail_df = ica_redux.merge(all_w_aep, left_on=['Compass Team ID'], right_on='COMPASS_TEAM_ID',how='right')

    #Detail Data Additional Calculations
    detail_df['AGENT_SPLIT'] =  detail_df['COMMISSION_EXPENSE'] / detail_df['GCI_LESS_REFERRAL']
    detail_df['COMPANY_SPLIT'] = 1+detail_df['AGENT_SPLIT']
    detail_df['NET_COMPANY_DOLLAR'] = detail_df['GCI_LESS_REFERRAL'] + detail_df['COMMISSION_EXPENSE']
    detail_df['AEP_ADJUSTED'].fillna(0,inplace=True)
    detail_df['AEP_TOTAL'] = detail_df['AEP'] + detail_df['AEP_ADJUSTED']
    detail_df['NCD_and_AEP_TOTAL'] = detail_df['NET_COMPANY_DOLLAR'] + detail_df['AEP_TOTAL']
    detail_df['ENDING_YEAR'] = detail_df['ENDING_PERIOD'].dt.year.astype(str)
    
    #ADD IN ADMIN/RESOURCE FEE SPLIT (430003/GCI LR) and Total NCD + Admin Fee


    #FILL NA 'tmp' for STRING COLS ELSE 0 if boolean, integer, unicode, float, and complex
    detail_df = detail_df.apply(lambda x: x.fillna(0) if x.dtype.kind in 'biufc' else x.fillna('tmp'))

    #Summary Build
    #tmp fillna to allow groupby
    grp = detail_df.groupby(['Compass Team ID',
                            'Account Team Name',
                            'ENDING_YEAR',
                            'MARKET_REGION',
                            'LOCATION_NAME',
                            'LISTING_TYPE',
                            'Market','Original Start Date']).agg({'COMPASS_DEALS_ID':'nunique',
                                                'COMMISSION_EXPENSE':sum,
                                                'GCI_LESS_REFERRAL':sum,
                                                'NET_COMPANY_DOLLAR':sum,
                                                'AEP_TOTAL':sum}).reset_index()

    grp['NCD_MARGIN'] = np.where(grp['GCI_LESS_REFERRAL']!=0, grp['NET_COMPANY_DOLLAR'] / grp['GCI_LESS_REFERRAL'], 0)

    piv = grp.pivot_table(values=['COMPASS_DEALS_ID',
                                'NCD_MARGIN',
                                'GCI_LESS_REFERRAL',
                                'COMMISSION_EXPENSE',
                                'NET_COMPANY_DOLLAR',
                                'AEP_TOTAL'],index=['Compass Team ID','Account Team Name','MARKET_REGION','LOCATION_NAME','LISTING_TYPE','Market','Original Start Date'],columns=['ENDING_YEAR'])

    piv.columns = ['_'.join(col).strip() for col in piv.columns.values]

    piv.reset_index(inplace=True)

    master_df = pd.DataFrame()

    #Still unknown if we need to isolate by market?
    #mkt_dict = {}

    for key,value in market_crosswalk.items():
        tmp_df = pd.DataFrame()
        if isinstance(value, list):
            for each in value:
                mini_df = piv[(piv['MARKET_REGION']==key) & (piv['Market']==each)].copy()
                tmp_df = tmp_df.append(mini_df)
            
            #mkt_dict[key] = tmp_df
            master_df = master_df.append(tmp_df)
            
        else:
            tmp_df = piv[(piv['MARKET_REGION']==key) & (piv['Market']==value)].copy()
            #mkt_dict[key] = tmp_df
            master_df = master_df.append(tmp_df)
    
    #########THIS SEDCTION IS HARD CODED, I DONT LIKE THIS################
    master_df['NUM_PRODUCING_PRINCIPALS_2019'] = np.where(master_df['GCI_LESS_REFERRAL_2019'].notnull(), 1,0)
    master_df['NUM_PRODUCING_PRINCIPALS_2020'] = np.where(master_df['GCI_LESS_REFERRAL_2020'].notnull(), 1,0)

    def get_cohort_totals(df, cohort_name, total_df = None):
        mkt_reg = df['MARKET_REGION'].iloc[0]
        cohort_df = df.groupby(['MARKET_REGION','Market','LOCATION_NAME','Compass Team ID','Account Team Name','LISTING_TYPE']).agg({'NUM_PRODUCING_PRINCIPALS_2019':sum,
                                                        'NUM_PRODUCING_PRINCIPALS_2020':sum,
                                                        'COMPASS_DEALS_ID_2019':sum, 
                                                        'COMPASS_DEALS_ID_2020':sum,
                                                        'GCI_LESS_REFERRAL_2019':sum, 
                                                        'GCI_LESS_REFERRAL_2020':sum,
                                                        'COMMISSION_EXPENSE_2019':sum,
                                                        'COMMISSION_EXPENSE_2020':sum,
                                                        'NET_COMPANY_DOLLAR_2019':sum,
                                                        'NET_COMPANY_DOLLAR_2020':sum,
                                                        'AEP_TOTAL_2019':sum,
                                                        'AEP_TOTAL_2020':sum
                                                        }).reset_index().rename(columns={'COMPASS_DEALS_ID_2019':'UNIT_TOTAL_2019',
                                                                                        'COMPASS_DEALS_ID_2020':'UNIT_TOTAL_2020',
                                                                                        'MARKET_REGION':'Analysis by Principal Recruitment Mix'})

        cohort_df['Analysis by Principal Recruitment Mix'] = cohort_name

        cohort_df['NCD_MARGIN_2019'] = cohort_df['NET_COMPANY_DOLLAR_2019']/cohort_df['GCI_LESS_REFERRAL_2019']
        cohort_df['NCD_MARGIN_2020'] = cohort_df['NET_COMPANY_DOLLAR_2020']/cohort_df['GCI_LESS_REFERRAL_2020']

        cohort_df['NCD_VARIANCE'] = cohort_df['NCD_MARGIN_2020']-cohort_df['NCD_MARGIN_2019']

        if total_df is None:
            cohort_df['PERCENT_OF_GCI_LESS_REFERRALS_2019'] = 1
            cohort_df['PERCENT_OF_GCI_LESS_REFERRALS_2020'] = 1
        else:
            cohort_df['PERCENT_OF_GCI_LESS_REFERRALS_2019'] = cohort_df['GCI_LESS_REFERRAL_2019']/total_df['GCI_LESS_REFERRAL_2019']
            cohort_df['PERCENT_OF_GCI_LESS_REFERRALS_2020'] = cohort_df['GCI_LESS_REFERRAL_2020']/total_df['GCI_LESS_REFERRAL_2020']
        
        cohort_df['MARKET_REGION'] = mkt_reg
        return cohort_df

    mkts = master_df['MARKET_REGION'].unique().tolist()

    master_summary_df = pd.DataFrame()
    for mkt in mkts:
        
        mkt_df = master_df[master_df['MARKET_REGION']==mkt].copy()
        
        for key,value in cohort_mix_dict.items():
            if value == 'all':
                sum_df = get_cohort_totals(mkt_df, key)
                master_summary_df = master_summary_df.append(sum_df)
            else:
                if any(isinstance(i, list) for i in value):
                    tmp = mkt_df[eval("(mkt_df['Original Start Date'] " + value[0][0] + " pd.to_datetime('" + value[0][1] + "')) \
                                        & (mkt_df['Original Start Date'] " + value[1][0] + " pd.to_datetime('" + value[1][1] + "'))")].copy()
                    #Dont like how this is done
                    sum_df = get_cohort_totals(tmp, key, master_summary_df[(master_summary_df['MARKET_REGION']==mkt) 
                                                                        & (master_summary_df['Analysis by Principal Recruitment Mix']=='Total Principal Cohorts')])
                    master_summary_df = master_summary_df.append(sum_df)
                else:
                    tmp = mkt_df[eval("mkt_df['Original Start Date'] " + value[0] + "pd.to_datetime('" + value[1] + "')")].copy()
                    
                    #Really don't like how this is done
                    sum_df = get_cohort_totals(tmp, key, master_summary_df[(master_summary_df['MARKET_REGION']==mkt) 
                                                                        & (master_summary_df['Analysis by Principal Recruitment Mix']=='Total Principal Cohorts')])
                    master_summary_df = master_summary_df.append(sum_df)

    #Fill in tmps and nulls
    detail_df = detail_df.replace("tmp",np.nan)

    detail_df['AGENT_SPLIT'].fillna(0, inplace=True)
    detail_df['AGENT_SPLIT'].replace(np.inf, 0,inplace=True)
    detail_df['AGENT_SPLIT'].replace(-np.inf, 0,inplace=True)
    detail_df['COMPANY_SPLIT'].fillna(0, inplace=True)
    detail_df['COMPANY_SPLIT'].replace(np.inf, 0,inplace=True)
    detail_df['COMPANY_SPLIT'].replace(-np.inf, 0,inplace=True)

    return {
        'deal_data' : detail_df,
        'principal_data' : master_df,
        'principal_mix_data' : master_summary_df
    }                
            
            

        
