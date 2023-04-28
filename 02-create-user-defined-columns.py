import datetime as dt
import sqlalchemy as sal
#from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from optilogic import pioneer
import math
import warnings
warnings.filterwarnings('ignore') # setting ignore as a parameter

####################### BEGIN USER INPUTS #######################
USER_NAME = 'graham.billey'
APP_KEY = 'op_NWQ3YjQ0NjktNTBjOC00M2JkLWE4NWEtNjM1NDBmODA5ODEw'
DB_NAME = 'PECO 2023-04 SOIP Opt (Volley)' # Cosmic Frog Model Name
LOSS_RATE = 0.975
Account_List = "Account List.xlsx"
Transfer_Matrix = "TransferMatrix_RenterToReturnLocation_PlanningGroups202109_to_202202_Smoothed - v2 (1).xlsx"
######################## END USER INPUTS ########################

# Suppress scientific notation globally
pd.options.display.float_format = '{:,.0f}'.format

# Start time of the process
START = dt.datetime.now()
print('The process started on: %s' %START)

#%%#################################################################################################

# Code that makes connection to the Cosmic Frog database specified at the user input above
api = pioneer.Api(auth_legacy = False, un=USER_NAME, appkey=APP_KEY)
connection_str = api.sql_connection_info(DB_NAME)
connection_string = 'postgresql://'+connection_str['raw']['user']+':'+connection_str['raw']['password']+'@'+connection_str['raw']['host']+':'+str(connection_str['raw']['port'])+'/'+connection_str['raw']['dbname']+'?sslmode=require'
engine = sal.create_engine(connection_string)
conn = engine.connect()

# List of all Cosmic Frog Model tables
db_tables = engine.table_names()

#%%#################################################################################################

###### MACRO 1

#try:
# Looping through all the Cosmic Frog Model tables and pulling the relevant ones
for i in db_tables:
    with engine.connect() as conn:
        trans = conn.begin()
        if i == 'customers':
            print('Reading customers table.')
            df_customers = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
            if 'id' in df_customers.columns:
                del df_customers['id']
        elif i == 'periods':
            print('Reading periods table.')
            df_periods = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
            if 'id' in df_periods.columns:
                del df_periods['id']
        elif i == 'customerdemand':
            print('Reading customerdemand table.')
            df_demand = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
            if 'id' in df_demand.columns:
                del df_demand['id']
        elif i == 'inventorypolicies':
            print('Reading inventorypolicies table.')
            df_inventorypolicies = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
            if 'id' in df_inventorypolicies.columns:
                del df_inventorypolicies['id']
        elif i == 'productionconstraints':
            print('Reading productionconstraints table.')
            df_productionconstraints = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
            if 'id' in df_productionconstraints.columns:
                del df_productionconstraints['id']
        trans.commit()



# Loop through each column in the productionconstraints table, and delete any columns added from 
# previous runs of this script. i.e. the "original_status_field" and "account_#####" columns.
df_productionconstraints_orig = df_productionconstraints.copy()

for j in df_productionconstraints.columns.tolist():
    if 'account_' in str(j.lower()) or 'original_status_field' in str(j.lower()):
        engine.execute('alter table %s drop column %s;' %('productionconstraints',str(j.lower())))
        del df_productionconstraints[j]

# Record the original values of the "status" column.
df_productionconstraints['original_status_field'] = df_productionconstraints['status']
df_productionconstraints_orig['original_status_field'] = df_productionconstraints_orig['status']

# Set "status" to "Include" for all rows with 'Repair_Capacity', 'MinRepairsPerDay', or 
# 'RepairsPerMonthVariance' in the "notes" column. Otherwise keep the existing status.
df_productionconstraints['status'] = df_productionconstraints[['status', 'notes']].apply(lambda x: 'Include' if (x[1] in ['Repair_Capacity', 'MinRepairsPerDay', 'RepairsPerMonthVariance']) else x[0], axis=1)
df_productionconstraints_orig['status'] = df_productionconstraints_orig[['status', 'notes']].apply(lambda x: 'Include' if (x[1] in ['Repair_Capacity', 'MinRepairsPerDay', 'RepairsPerMonthVariance']) else x[0], axis=1)


#%%#################################################################################################

############ MACRO 4

acc_list = pd.read_excel(Account_List, sheet_name='Selected Accounts')
acc_list.raw = acc_list.copy()

acc_list = acc_list[acc_list['Evaulate'].str.lower() == 'y'][['Corporate Code', 'Corporate Description', 'Evaulate']]

acc_list = acc_list.drop_duplicates(subset=['Corporate Code', 'Corporate Description'])
acc_list['Corporate Code'] = acc_list['Corporate Code'].astype(str)

df_customers1 = df_customers[['customername', 'corpcode']]
df_demand2 = pd.merge(df_demand, df_customers1, on = 'customername', how = 'left')
df_demand2['corpcode'] = df_demand2['corpcode'].astype(str)

# Only look at accounts that have planned issues in the Opt model.
df_selected_accounts = pd.merge(acc_list, df_demand2, left_on = 'Corporate Code', right_on = 'corpcode', how = 'inner')
df_selected_accounts = df_selected_accounts[['Corporate Code', 'Corporate Description', 'Evaulate']].drop_duplicates()

#%%#################################################################################################

############ MACRO 3

print('Reading transfer matrix data...')
transfer_matrix = pd.read_excel(Transfer_Matrix, sheet_name='TransferMatrix')
transfer_matrix_raw = transfer_matrix.copy()
print('\tDone.')

print('Aggregating transfer matrix data...')
transfer_matrix = transfer_matrix.groupby(['Renter Corp Code', 'Return Loc Code'])['Return Volume'].apply(sum)  # Removing 'Renter Corp Desc' as well.

transfer_matrix = transfer_matrix.to_frame()
transfer_matrix = transfer_matrix.reset_index()
transfer_matrix['Renter Corp Code'] = transfer_matrix['Renter Corp Code'].astype(str)
print('\tDone.')

#%%#################################################################################################
print('Looping through accounts to update ProductionConstraints...')
for account in df_selected_accounts['Corporate Code'].unique():

    print('Account: ',account)
    
    ##### MACRO 2

    # This section just creates dataframes for returns, new manufactured pallets, and converts datatypes.
    df_productionconstraints['constraintvalue'] = df_productionconstraints['constraintvalue'].astype(float)
    df_returns = df_productionconstraints[df_productionconstraints['facilityname'].str.startswith('R_', na=False)]
    df_returns = df_returns.groupby('periodname')['constraintvalue'].apply(sum)   # Calc total returns each month.
    df_RFU_NEW = df_productionconstraints[(~df_productionconstraints['facilityname'].str.startswith('R_', na=False)) & (df_productionconstraints['productname'] == 'RFU_NEW')]
    df_RFU_NEW1 = df_RFU_NEW.groupby('periodname')['constraintvalue'].apply(sum)  # Calc total new manufactured pallets each month.
    df_returns = df_returns.to_frame()
    df_returns = df_returns.reset_index()
    df_RFU_NEW1 = df_RFU_NEW1.to_frame()
    df_RFU_NEW1 = df_RFU_NEW1.reset_index()

    # Get total demand (issue) quantity by month.
    df_demand['quantity'] = df_demand['quantity'].astype(float)
    df_demand1 = df_demand.groupby('periodname')['quantity'].apply(sum)
    df_demand1 = df_demand1.to_frame()
    df_demand1 = df_demand1.reset_index()
    
    # Create an initial inventory dataframe.
    df_inventorypolicies['initialinventory'] = df_inventorypolicies['initialinventory'].astype(float)
    initial_inventory = df_inventorypolicies['initialinventory'].sum()  # Total inventory at the start of the model run.
    df_initial_inventory = pd.DataFrame({'periodname':'Period 01', 'initialinventory':initial_inventory}, index = [0])

    # quantity = Issues
    # constraintvalue & constraintvalue_x = Returns
    # constraintvalue_y = New Manufactured Pallets
    df_final = df_demand1.merge(df_returns,on='periodname', how = 'left')  # Merge issues and returns into one dataframe
    df_final = df_final.merge(df_initial_inventory, on = 'periodname', how = 'left')
    
    df_final = df_final.merge(df_RFU_NEW1, on = 'periodname', how = 'outer')    
    
    df_final = df_final.merge(df_periods[['periodname', 'workingdays']], on = 'periodname', how = 'left')
    df_final = df_final.rename(columns={'constraintvalue_x':'constraintvalue', 'constraintvalue_y':'MFG'})
    df_final[['quantity', 'constraintvalue', 'initialinventory', 'MFG', 'workingdays']] = df_final[['quantity', 'constraintvalue', 'initialinventory', 'MFG', 'workingdays']].fillna(0)
    df_final['Beginning Inventory'] = df_final[['quantity', 'constraintvalue', 'MFG', 'initialinventory']].apply(lambda x: x[3] if x[3] > 0 else 0, axis = 1)

    # Drop row 2023, added from joining to df_RFU_NEW1.
    df_final = df_final[~(df_final['periodname']=='2023')].reset_index(drop=True)

    # Beginning inventory for a month = beginning inventory of last month, + returns + MFG - issues.
    # constraintvalue = returns
    # quantity = issues
    for i in list(df_final.index.values):
        if df_final.loc[i, 'Beginning Inventory'] == 0:
            df_final.loc[i, 'Beginning Inventory'] = df_final.loc[i-1, 'Beginning Inventory'] + \
            df_final.loc[i-1, 'constraintvalue'] + \
            df_final.loc[i-1, 'MFG'] - \
            df_final.loc[i-1, 'quantity']
    
    df_final['workingdays'] = df_final['workingdays'].astype(int)
    
    # Ending inventory = beginning inventory + returns + mfg - issues
    df_final['Ending Inventory'] = df_final[['quantity', 'constraintvalue', 'MFG', 'Beginning Inventory']].apply(lambda x: x[3] + x[1] + x[2]- x[0], axis = 1)
    
    # Gap_6Days = (issues per day * 6) minus beginning inventory. 
    df_final['Gap_6Days'] = df_final[['quantity', 'workingdays', 'Beginning Inventory']].apply(lambda x: round(((x[0]/x[1])*6) - x[2]) if x[1] != 0 else np.nan, axis = 1)
    
    # Keep only the next 12 months. i.e. drop period 13 through 19.
    df_final = df_final[df_final['periodname'].apply(lambda x: int(x.split(' ')[1])) <= 12].reset_index(drop = True)
    df_final_12_mos = df_final.copy()   # Make a copy since we're overwriting the df with just a max row.
    
    # Keep only the row where 'Gap_6Days' is maximum.
    df_final = df_final[df_final.Gap_6Days == df_final.Gap_6Days.max()][['Gap_6Days', 'periodname', 'quantity', 'constraintvalue', 'initialinventory']]
    
    df_final['NeededMFG'] = df_final[['Gap_6Days', 'periodname']].apply(lambda x: x[0]/(int(x[1].split(' ')[1])-1) if int(x[1].split(' ')[1])-1 != 0 else np.nan, axis = 1)
    df_final['Periods'] = df_final['periodname'].apply(lambda x: int(x.split(' ')[1])-1)
    
    df_final = df_final.loc[df_final.index.repeat(df_final['Periods'].iloc[0])].reset_index(drop = True)
    df_final['RowCount'] = df_final.index + 1
    
    
    if len(df_final) > 0:
        df_final['periodname'] = df_final[['periodname', 'RowCount']].apply(lambda x: 'Period 0%d' %(int(x[0].split(' ')[1]) - x[1]), axis = 1)
    
    df_final = df_final[['NeededMFG', 'periodname', 'quantity', 'constraintvalue', 'initialinventory']]

    # Alabama manufacturing
    df_RFU_NEW_specific_depot = df_RFU_NEW[df_RFU_NEW['facilityname'] == 'D_USA35490_54027']
    df_final = pd.merge(df_RFU_NEW_specific_depot, df_final[['periodname', 'NeededMFG']], on = 'periodname', how = 'left')
    df_final['NeededMFG'] = df_final['NeededMFG'].fillna(0)
    df_final['adjusted_constraintvalue'] = df_final[['constraintvalue', 'NeededMFG']].apply(lambda x: x[0] + x[1], axis = 1)
    del df_final['NeededMFG']
    df_final = pd.concat([df_final, df_RFU_NEW[~(df_RFU_NEW['facilityname'] == 'D_USA35490_54027')]])
    
    # transfer_matrix has the number of pallets moving from a renter CORP code to return LOCATION code.
    transfer_matrix1 = pd.merge(df_selected_accounts[df_selected_accounts['Corporate Code'] == account], transfer_matrix, left_on = 'Corporate Code', right_on = 'Renter Corp Code', how = 'inner')
    transfer_matrix2 = transfer_matrix1.groupby(['Corporate Code', 'Corporate Description'])['Return Volume'].apply(sum)
    transfer_matrix2 = transfer_matrix2.to_frame()
    transfer_matrix2 = transfer_matrix2.reset_index()
    transfer_matrix2 = transfer_matrix2.rename(columns = {'Return Volume':'Total Volume'})
    transfer_matrix3 = pd.merge(transfer_matrix1, transfer_matrix2[['Corporate Code', 'Total Volume']], on = 'Corporate Code', how = 'inner')
    transfer_matrix3['PercentIssueVolume'] = transfer_matrix3[['Return Volume', 'Total Volume']].apply(lambda x: x[0]/x[1], axis = 1)
    del transfer_matrix3['Total Volume']

    # Get total demand for all customers with the selected corp code, grouped by period.
    df_customers2 = df_customers[['customername', 'loccode', 'corpcode', 'corpname']]
    df_demand3 = pd.merge(df_demand, df_customers2, on = 'customername', how = 'left')
    df_demand3['corpcode'] = df_demand3['corpcode'].astype(str)
    df_demand4 = pd.merge(df_demand3, df_selected_accounts[df_selected_accounts['Corporate Code'] == account][['Corporate Code', 'Corporate Description']], left_on = 'corpcode', right_on = 'Corporate Code', how = 'inner')
    df_demand4['quantity'] = df_demand4['quantity'].astype(float)
    df_demand5 = df_demand4.groupby(['Corporate Code', 'Corporate Description', 'periodname'])['quantity'].apply(sum)
    df_demand5 = df_demand5.to_frame()
    df_demand5 = df_demand5.reset_index()

    
    # Get production constraints for all Return locations.
    df_productionconstraints1 = df_productionconstraints[df_productionconstraints['facilityname'].str.startswith('R_', na=False)]
    # Join with demand (Issues)
    df_productionconstraints2 = pd.merge(df_productionconstraints1, df_demand5[['periodname', 'quantity']], on = 'periodname', how = 'inner')
    # Extract Loc code from the production constraint.
    df_productionconstraints2['LocCode'] = df_productionconstraints2['facilityname'].apply(lambda x: x.split('_')[2])
    df_productionconstraints2['LocCode'] = df_productionconstraints2['LocCode'].astype(str)
    
    # Merge Production Constraints table with the % Issue volume from Transfer Matrix.
    transfer_matrix3['Return Loc Code'] = transfer_matrix3['Return Loc Code'].astype(str)
    df_productionconstraints3 = pd.merge(df_productionconstraints2, transfer_matrix3[['Return Loc Code', 'PercentIssueVolume']], left_on = 'LocCode', right_on = 'Return Loc Code', how = 'left')
    df_productionconstraints3['PercentIssueVolume'] = df_productionconstraints3['PercentIssueVolume'].fillna(0)
    df_productionconstraints3['Issues x PctIssueVol'] = df_productionconstraints3['quantity'] * df_productionconstraints3['PercentIssueVolume']

    # Group Returns by facility and period.
    df_productionconstraints2['constraintvalue'] = df_productionconstraints2['constraintvalue'].astype(float)
    df_productionconstraints4 = df_productionconstraints2.groupby(['facilityname', 'periodname'])['constraintvalue'].apply(sum)
    df_productionconstraints4 = df_productionconstraints4.to_frame()
    df_productionconstraints4 = df_productionconstraints4.reset_index()
 
    df_productionconstraints5 = pd.merge(df_productionconstraints3, df_productionconstraints4, on = ['facilityname', 'periodname'], how = 'inner')
    df_productionconstraints5['ConstraintValueAdjusted'] = df_productionconstraints5[['constraintvalue_x', 'constraintvalue_y', 'quantity', 'PercentIssueVolume']].apply(lambda x: round(x[0]-min((x[0]/x[1])*(x[2]*x[3]*LOSS_RATE),x[0]), 6), axis = 1)

    # Create a DF with the adjusted returns at individual depots.
    # The constraint value we want is called 'production_constraint_value'
    df_productionconstraints6 = df_productionconstraints5.rename(columns = {'ConstraintValueAdjusted':'adjusted_constraintvalue'})
    df_productionconstraints6['constraintvalue_x'] = df_productionconstraints6['constraintvalue_x'].fillna(0)
    df_productionconstraints6 = df_productionconstraints6.rename(columns = {'constraintvalue_x':'constraintvalue'})

    # Create a DF with the repair and new manufacturing production constraints.
    df_productionconstraints7 = df_productionconstraints[~df_productionconstraints['facilityname'].str.startswith('R_', na=False)]
    
    # Repairs for the 'Depot' group.
    df_productionconstraints8 = df_productionconstraints7[df_productionconstraints7['facilityname'] == 'Depot']
    
    # Repairs at individual depots AND manufacturing at indivisual depots and total.
    df_productionconstraints9 = df_productionconstraints7[~(df_productionconstraints7['facilityname'] == 'Depot')]
    
    # Repairs at individual depots.
    df_productionconstraints10 = df_productionconstraints9[~(df_productionconstraints9['productname'] == 'RFU_NEW')]
    df_productionconstraints10['adjusted_constraintvalue'] = df_productionconstraints10['constraintvalue']
   
    # RFU and WIP returns grouped by period. 
    # constraintvalue_x = original constraint value.
    # ConstraintValueAdjusted = adjusted value after removing returns from the specified account.
    df_productionconstraints11 = df_productionconstraints5.groupby(['productname', 'periodname'])['constraintvalue_x', 'ConstraintValueAdjusted'].apply(sum)
    df_productionconstraints11 = df_productionconstraints11.reset_index()

    # Calculate the amount of WIP being removed. This means there will be fewer repairs as well.
    df_productionconstraints11['WIPQuantity'] = df_productionconstraints11[['constraintvalue_x', 'ConstraintValueAdjusted']].apply(lambda x: math.floor(x[0] -x[1]), axis = 1)
    df_productionconstraints12 = df_productionconstraints11[df_productionconstraints11['productname'] == 'WIP']

    # Merge repairs for the Depot group with the WIP quantity we need to exclude.
    df_productionconstraints13 = pd.merge(df_productionconstraints8, df_productionconstraints12, on = 'periodname', how = 'inner')

    # Reduce the repairs for the Depot group by the WIP quantity we are excluding for each period.
    # Use constraintvalue_x because this is the total for each depot summed individually.
    df_productionconstraints13['production constraint value adjusted'] = df_productionconstraints13[['constraintvalue_x', 'WIPQuantity']].apply(lambda x: x[0] - x[1], axis = 1)
    df_productionconstraints13.rename(columns={'production constraint value adjusted':'adjusted_constraintvalue',
                                               'productname_y':'productname'}, inplace=True)
   
    # Create the final production constraints dataframe by unioning:
    # df_productionconstraints10 : Repairs at individual depots.       NOTE: This isn't using an adjusted repair value.
    #                            : Want 'adjusted_constraintvalue'
    #
    # df_final                   : Additional manufacturing required at the Alabama mfg facility (to eliminate low inventory problems).
    #                            : Want 'adjusted_constraintvalue'
    #
    # df_productionconstraints6  : Adjusted returns at individual depots.
    #                            : Want 'adjusted_constraintvalue'
    #
    # df_productionconstraints13 : Adjusted repairs for the 'Depots' group.
    #                            : Want 'adjusted_constraintvalue'
   
    
    final_columns = ['facilityname', 'facilitynamegroupbehavior', 'productname',
           'productnamegroupbehavior', 'periodname', 'periodnamegroupbehavior',
           'bomname', 'bomnamegroupbehavior', 'processname',
           'processnamegroupbehavior', 'constrainttype', 'constraintvalue',
           'constraintvalueuom', 'status', 'notes', 'soipquantity',
           'conversionnotes', 'original_status_field', 'adjusted_constraintvalue']
    
    df_productionconstraints_final = pd.concat([df_productionconstraints10[final_columns], 
                                                df_final[final_columns], 
                                                df_productionconstraints6[final_columns], 
                                                df_productionconstraints13[final_columns],
                                                ])
    
    df_productionconstraints_final[df_productionconstraints_final['constraintvalue'] != df_productionconstraints_final['adjusted_constraintvalue']]
    df_productionconstraints_final['status'] = df_productionconstraints_final['original_status_field']
    df_productionconstraints_final['account_%s'%account] = df_productionconstraints_final['adjusted_constraintvalue']
    df_productionconstraints_final.drop(columns=['adjusted_constraintvalue'], inplace=True)

    join_cols = ['facilityname', 'facilitynamegroupbehavior', 'productname',
           'productnamegroupbehavior', 'periodname', 'periodnamegroupbehavior',
           'bomname', 'bomnamegroupbehavior', 'processname',
           'processnamegroupbehavior', 'constrainttype',
           'constraintvalueuom', 'status', 'notes', 'soipquantity',
           'conversionnotes', 'original_status_field']

    df_productionconstraints_orig = df_productionconstraints_orig.merge(df_productionconstraints_final, 
                                                                        how='left',
                                                                        left_on=join_cols ,
                                                                        right_on=join_cols ,
                                                                        suffixes=('', '_DROP'))
    
    df_productionconstraints_orig.drop(df_productionconstraints_orig.filter(regex='_DROP$').columns, axis=1, inplace=True)


#%%#################################################################################################
print('Uploading data to Cosmic Frog.')

df_productionconstraints = df_productionconstraints_orig.copy()

for col in df_productionconstraints.columns.tolist():
    if 'account_' in col or 'original_status_field' in col:
        engine.execute('alter table %s add column "%s" varchar(200)' %('productionconstraints',str(col)))

            
engine.execute('delete from productionconstraints')
df_productionconstraints.to_sql('productionconstraints', con=engine, if_exists='append',index=False)
conn.close()

#%%#################################################################################################

END = dt.datetime.now()
print(END)
tot_sec = (END-START).total_seconds()

print('The process ended on: %s' %END)
print('It took %d minutes and %d seconds to finish the process' %(int(tot_sec//60), int(tot_sec - (tot_sec//60)*60)))