# -*- coding: utf-8 -*-

# imports
import pandas as pd
import numpy as np
import datetime as dt
import generators as gi_gt
import chainladder as cl
from dateutil.relativedelta import relativedelta


def summary_triangles(data, reporting_date):
    '''
    Remove all data which is not know at the reporting date and convert data into triangles, for further analysis
    Returns 'chainladder' triangles
    '''
    #    
    #fileter out all data not known at reporting date
    #
    
    # create masks
    data_triangles = data.copy()
    date_mask_notpaid = (data_triangles['Claim_payment_date']-reporting_date)/np.timedelta64(1,'D')>0
    date_mask_notreported = (data_triangles['Claim_report_date']-reporting_date)/np.timedelta64(1,'D')>0
    date_mask_paid = (data_triangles['Claim_payment_date']-reporting_date)/np.timedelta64(1,'D')<=0
    date_mask_reported = (data_triangles['Claim_report_date']-reporting_date)/np.timedelta64(1,'D')<=0

    date_mask_notwritten = (data_triangles['Start_date']-reporting_date)/np.timedelta64(1,'D')>0
    
    # clear paid and reported data which is unknown at reporting date
    data_triangles.loc[date_mask_notpaid, 'Claim_payment_date'] = np.nan
    data_triangles.loc[date_mask_notreported, ['Claim_report_date','Claim_incident_date', 'Claim_value', 'Claim_value_gu']] = np.nan

    # add counts for paid and reported
    data_triangles['Claim_count_reported'] = 0
    data_triangles['Claim_count_paid'] = 0
    data_triangles.loc[date_mask_reported, 'Claim_count_reported'] = 1
    data_triangles.loc[date_mask_paid, 'Claim_count_paid'] = 1

    # remove unwritten policies
    data_policies = data_triangles.loc[~date_mask_notwritten].copy()    

    tri_paid = cl.Triangle(data_policies, 
                    origin='Start_date',
                    index='Class_name',
                    development='Claim_payment_date',
                    columns='Claim_value',
                    cumulative=False).incr_to_cum().to_frame().reset_index().melt(id_vars="index", var_name='Development Month', value_name='Paid Value').rename({'index':'Origin Month'}, axis='columns')


    tri_paid_count = cl.Triangle(data_policies, 
                    origin='Start_date',
                    index='Class_name',
                    development='Claim_payment_date',
                    columns='Claim_count_paid',
                    cumulative=False).incr_to_cum().to_frame().reset_index().melt(id_vars="index", var_name='Development Month', value_name='Paid Count').rename({'index':'Origin Month'}, axis='columns')

    
    tri_reported = cl.Triangle(data_policies, 
                    origin='Start_date',
                    index='Class_name',
                    development='Claim_report_date',
                    columns='Claim_value',
                    cumulative=False).incr_to_cum().to_frame().reset_index().melt(id_vars="index", var_name='Development Month', value_name='Reported Value').rename({'index':'Origin Month'}, axis='columns')


    tri_reported_count = cl.Triangle(data_policies, 
                    origin='Start_date',
                    index='Class_name',
                    development='Claim_report_date',
                    columns='Claim_count_reported',
                    cumulative=False).incr_to_cum().to_frame().reset_index().melt(id_vars="index", var_name='Development Month', value_name='Reported Count').rename({'index':'Origin Month'}, axis='columns')


    tri_premium = cl.Triangle(data_policies, 
                    origin='Start_date',
                    index='Class_name',
                    development='Policy_premium_date',
                    columns='Policy_premium',
                    cumulative=False).incr_to_cum().to_frame().reset_index().melt(id_vars="index", var_name='Development Month', value_name='Premium Value').rename({'index':'Origin Month'}, axis='columns')

    #join all triangles together into a single dataframe
    join_cols = ['Origin Month', 'Development Month']
    tri_all = tri_paid.merge(tri_paid_count, on=join_cols)
    tri_all = tri_paid.merge(tri_reported, on=join_cols)
    tri_all = tri_all.merge(tri_reported_count, on=join_cols)
    tri_all = tri_all.merge(tri_premium, on=join_cols)
        
    return tri_all, data_policies

#%%

# run code to test
if __name__ == '__main__':

    uw_start_date = dt.datetime.strptime('01/01/2019', '%d/%m/%Y')
    reporting_date = dt.datetime.strptime('31/12/2019', '%d/%m/%Y')

    db_data = gi_gt.generate_ultimate_portfolio(class_name='Motor', historic_years=10, uw_start_date=uw_start_date)
    db_asat_data = gi_gt.asat_filtering(db_data, reporting_date)
    
    stats = gi_gt.summary_stats(db_data,db_asat_data)

    #
    # build chainladder triangles
    #

    triangles_all, data_policies = summary_triangles(db_asat_data, reporting_date)
    
    #
    # export as csv file
    #
    
    triangles_all.to_csv('triangles.csv')
                
    
        
        
        
    
    
    
    
    
    
    
    