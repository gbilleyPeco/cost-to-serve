import datetime as dt
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from optilogic import pioneer
import math
import warnings
warnings.filterwarnings('ignore') # setting ignore as a parameter


# Start time of the process
START = dt.datetime.now()
print('The process started on: %s' %START)

# COSMIC FROG USER INPUT
USER_NAME = 'optiscience'
APP_KEY = 'op_YWUyOGRiZDUtNjAyOC00YjdmLTk1ZTQtM2U0YzMxMjEzODRh'
DB_NAME = 'PECO_AS_INTENDED_03222023' # Cosmic Frog Model Name


# Local Filepaths
Account_List = "Account List.xlsx"
Transfer_Matrix = "TransferMatrix_RenterToReturnLocation_PlanningGroups202109_to_202202_Smoothed - v2 (1).xlsx"

# Code that makes connection to the Cosmic Frog database specified at the user input above
api = pioneer.Api(auth_legacy = False, un=USER_NAME, appkey=APP_KEY)
connection_str = api.sql_connection_info(DB_NAME)
connection_string = 'postgresql://'+connection_str['raw']['user']+':'+connection_str['raw']['password']+'@'+connection_str['raw']['host']+':'+str(connection_str['raw']['port'])+'/'+connection_str['raw']['dbname']+'?sslmode=require'
engine = sal.create_engine(connection_string)
conn = engine.connect()

# List of all Cosmic Frog Model tables
db_tables = engine.table_names()


###### MACRO 1

try:
    # Looping through all the Cosmic Frog Model tables and pulling the relevant ones
    for i in db_tables:
        with engine.connect() as conn:
            trans = conn.begin()
            if i == 'customers':
                df_customers = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
                if 'id' in df_customers.columns:
                    del df_customers['id']
            elif i == 'periods':
                df_periods = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
                if 'id' in df_periods.columns:
                    del df_periods['id']
            elif i == 'customerdemand':
                df_demand = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
                if 'id' in df_demand.columns:
                    del df_demand['id']
            elif i == 'inventorypolicies':
                df_inventorypolicies = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
                if 'id' in df_inventorypolicies.columns:
                    del df_inventorypolicies['id']
            elif i == 'productionconstraints':
                df_productionconstraints = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
                if 'id' in df_productionconstraints.columns:
                    del df_productionconstraints['id']
            trans.commit()
    
    for j in df_productionconstraints.columns.tolist():
        if 'account_' in str(j.lower()) or 'original_status_field' in str(j.lower()):
            engine.execute('alter table %s drop column %s;' %('productionconstraints',str(j.lower())))
            del df_productionconstraints[j]
    df_productionconstraints['original_status_field'] = df_productionconstraints['status']
    df_productionconstraints['status'] = df_productionconstraints[['status', 'notes']].apply(lambda x: 'Include' if (x[1] in ['Repair_Capacity', 'MinRepairsPerDay', 'RepairsPerMonthVariance']) else x[0])

    
    ############ MACRO 4

    acc_list = pd.read_excel(Account_List, sheet_name='Selected Accounts')
    acc_list = acc_list[acc_list['Evaulate'].str.lower() == 'y'][['Corporate Code', 'Corporate Description', 'Evaulate']]
    
    acc_list = acc_list.drop_duplicates(subset=['Corporate Code', 'Corporate Description'])
    acc_list['Corporate Code'] = acc_list['Corporate Code'].astype(str)
    
    df_customers1 = df_customers[['customername', 'corpcode']]
    df_demand2 = pd.merge(df_demand, df_customers1, on = 'customername', how = 'left')
    df_demand2['corpcode'] = df_demand2['corpcode'].astype(str)

    df_selected_accounts = pd.merge(acc_list, df_demand2, left_on = 'Corporate Code', right_on = 'corpcode', how = 'inner')
    df_selected_accounts = df_selected_accounts[['Corporate Code', 'Corporate Description', 'Evaulate']].drop_duplicates()

    ############ MACRO 3

    transfer_matrix = pd.read_excel(Transfer_Matrix, sheet_name='TransferMatrix')
    transfer_matrix = transfer_matrix.groupby(['Renter Corp Code', 'Renter Corp Desc', 'Return Loc Code'])['Return Volume'].apply(sum) # adding Return Loc Description creates Return Loc Code duplicates
    transfer_matrix = transfer_matrix.to_frame()
    transfer_matrix = transfer_matrix.reset_index()
    transfer_matrix['Renter Corp Code'] = transfer_matrix['Renter Corp Code'].astype(str)

    # df_customers2 = df_customers[['customername', 'loccode', 'corpcode', 'corpname']]
    # df_demand3 = pd.merge(df_demand, df_customers2, on = 'customername', how = 'left')
    # df_demand3['corpcode'] = df_demand3['corpcode'].astype(str)

    for account in df_selected_accounts['Corporate Code'].unique():
        
        ##### MACRO 2

        df_productionconstraints['constraintvalue'] = df_productionconstraints['constraintvalue'].astype(float)
        df_renters = df_productionconstraints[df_productionconstraints['facilityname'].str.startswith('R_', na=False)]
        df_renters = df_renters.groupby('periodname')['constraintvalue'].apply(sum)
        df_RFU_NEW = df_productionconstraints[(~df_productionconstraints['facilityname'].str.startswith('R_', na=False)) & (df_productionconstraints['productname'] == 'RFU_NEW')]
        df_RFU_NEW1 = df_RFU_NEW.groupby('periodname')['constraintvalue'].apply(sum)
        df_renters = df_renters.to_frame()
        df_renters = df_renters.reset_index()
        df_RFU_NEW1 = df_RFU_NEW1.to_frame()
        df_RFU_NEW1 = df_RFU_NEW1.reset_index()


        df_demand['quantity'] = df_demand['quantity'].astype(float)
        df_demand1 = df_demand.groupby('periodname')['quantity'].apply(sum)
        df_demand1 = df_demand1.to_frame()
        df_demand1 = df_demand1.reset_index()


        df_inventorypolicies['initialinventory'] = df_inventorypolicies['initialinventory'].astype(float)
        df_inventorypolicies = df_inventorypolicies['initialinventory'].sum()
        df_inventorypolicies = pd.DataFrame({'periodname':'Period 01', 'initialinventory':df_inventorypolicies}, index = [0])


        df_final = df_demand1.merge(df_renters,on='periodname', how = 'left').merge(df_inventorypolicies, on = 'periodname', how = 'left').merge(df_RFU_NEW1, on = 'periodname', how = 'outer').merge(df_periods[['periodname', 'workingdays']], on = 'periodname', how = 'left')
        df_final = df_final.rename(columns={'constraintvalue_x':'constraintvalue', 'constraintvalue_y':'MFG'})
        df_final[['quantity', 'constraintvalue', 'initialinventory', 'MFG', 'workingdays']] = df_final[['quantity', 'constraintvalue', 'initialinventory', 'MFG', 'workingdays']].fillna(0)
        df_final['Beginning Inventory'] = df_final[['quantity', 'constraintvalue', 'MFG', 'initialinventory']].apply(lambda x: x[3] if x[3] > 0 else 0, axis = 1)
        last_row = df_final.tail(1)
        df_final = df_final.drop(df_final.index[len(df_final)-1])
        df_final = pd.concat([last_row, df_final]).reset_index(drop = True)
        for i in list(df_final.index.values):
            if df_final.loc[i, 'Beginning Inventory'] == 0 and i != 0:
                df_final.loc[i, 'Beginning Inventory'] = df_final.loc[i-1, 'Beginning Inventory'] + df_final.loc[i-1, 'constraintvalue'] + df_final.loc[i-1, 'MFG'] - df_final.loc[i-1, 'quantity']
        df_final['workingdays'] = df_final['workingdays'].astype(int)
        df_final['Ending Inventory'] = df_final[['quantity', 'constraintvalue', 'MFG', 'Beginning Inventory']].apply(lambda x: x[3] + x[1] + x[2]- x[0], axis = 1)
        df_final['Gap_6Days'] = df_final[['quantity', 'workingdays', 'Beginning Inventory']].apply(lambda x: round(((x[0]/x[1])*6) - x[2]) if x[1] != 0 else np.nan, axis = 1)
        df_final = df_final[df_final['periodname'].str.contains('Period')]
        df_final = df_final[df_final['periodname'].apply(lambda x: int(x.split(' ')[1])) <= 12].reset_index(drop = True)
        df_final = df_final[df_final.Gap_6Days == df_final.Gap_6Days.max()][['Gap_6Days', 'periodname', 'quantity', 'constraintvalue', 'initialinventory']]
        df_final['NeededMFG'] = df_final[['Gap_6Days', 'periodname']].apply(lambda x: x[0]/(int(x[1].split(' ')[1])-1) if int(x[1].split(' ')[1])-1 != 0 else np.nan, axis = 1)
        df_final['Periods'] = df_final['periodname'].apply(lambda x: int(x.split(' ')[1])-1)
        df_final = df_final.loc[df_final.index.repeat(df_final['Periods'].iloc[0])].reset_index(drop = True)
        df_final['RowCount'] = df_final.index + 1
        if len(df_final) > 0:
            df_final['periodname'] = df_final[['periodname', 'RowCount']].apply(lambda x: 'Period 0%d' %(int(x[0].split(' ')[1]) - x[1]), axis = 1)
        df_final = df_final[['NeededMFG', 'periodname', 'quantity', 'constraintvalue', 'initialinventory']]

        df_RFU_NEW_specific_depot = df_RFU_NEW[df_RFU_NEW['facilityname'] == 'D_USA35490_54027']

        df_final = pd.merge(df_RFU_NEW_specific_depot, df_final[['periodname', 'NeededMFG']], on = 'periodname', how = 'left')
        df_final['NeededMFG'] = df_final['NeededMFG'].fillna(0)
        df_final['constraintvalue'] = df_final[['constraintvalue', 'NeededMFG']].apply(lambda x: x[0] + x[1], axis = 1)
        del df_final['NeededMFG']
        df_final = pd.concat([df_final, df_RFU_NEW[~(df_RFU_NEW['facilityname'] == 'D_USA35490_54027')]])



        print(account)
        #print(df_productionconstraints[[i for i in df_productionconstraints.columns.tolist() if 'account_' in i]])
        transfer_matrix1 = pd.merge(df_selected_accounts[df_selected_accounts['Corporate Code'] == account], transfer_matrix, left_on = 'Corporate Code', right_on = 'Renter Corp Code', how = 'inner')
        transfer_matrix2 = transfer_matrix1.groupby(['Corporate Code', 'Corporate Description'])['Return Volume'].apply(sum)
        transfer_matrix2 = transfer_matrix2.to_frame()
        transfer_matrix2 = transfer_matrix2.reset_index()
        transfer_matrix2 = transfer_matrix2.rename(columns = {'Return Volume':'Total Volume'})
        transfer_matrix3 = pd.merge(transfer_matrix1, transfer_matrix2[['Corporate Code', 'Total Volume']], on = 'Corporate Code', how = 'inner')
        transfer_matrix3['PercentIssueVolume'] = transfer_matrix3[['Return Volume', 'Total Volume']].apply(lambda x: x[0]/x[1], axis = 1)
        del transfer_matrix3['Total Volume']

        df_customers2 = df_customers[['customername', 'loccode', 'corpcode', 'corpname']]
        df_demand3 = pd.merge(df_demand, df_customers2, on = 'customername', how = 'left')
        df_demand3['corpcode'] = df_demand3['corpcode'].astype(str)
        df_demand4 = pd.merge(df_demand3, df_selected_accounts[df_selected_accounts['Corporate Code'] == account][['Corporate Code', 'Corporate Description']], left_on = 'corpcode', right_on = 'Corporate Code', how = 'inner')
        df_demand4['quantity'] = df_demand4['quantity'].astype(float)
        df_demand5 = df_demand4.groupby(['Corporate Code', 'Corporate Description', 'periodname'])['quantity'].apply(sum)
        df_demand5 = df_demand5.to_frame()
        df_demand5 = df_demand5.reset_index()

        
        df_productionconstraints1 = df_productionconstraints[df_productionconstraints['facilityname'].str.startswith('R_', na=False)]
       
        df_productionconstraints2 = pd.merge(df_productionconstraints1, df_demand5[['periodname', 'quantity']], on = 'periodname', how = 'inner')
        
        df_productionconstraints2['LocCode'] = df_productionconstraints2['facilityname'].apply(lambda x: x.split('_')[2])
        
        df_productionconstraints2['LocCode'] = df_productionconstraints2['LocCode'].astype(str)
        
        transfer_matrix3['Return Loc Code'] = transfer_matrix3['Return Loc Code'].astype(str)
        df_productionconstraints3 = pd.merge(df_productionconstraints2, transfer_matrix3[['Return Loc Code', 'PercentIssueVolume']], left_on = 'LocCode', right_on = 'Return Loc Code', how = 'left')
        
        df_productionconstraints3['PercentIssueVolume'] = df_productionconstraints3['PercentIssueVolume'].fillna(0)
       
        
        df_productionconstraints2['constraintvalue'] = df_productionconstraints2['constraintvalue'].astype(float)

        df_productionconstraints4 = df_productionconstraints2.groupby(['facilityname', 'periodname'])['constraintvalue'].apply(sum)
        df_productionconstraints4 = df_productionconstraints4.to_frame()
        df_productionconstraints4 = df_productionconstraints4.reset_index()
        

        df_productionconstraints5 = pd.merge(df_productionconstraints3, df_productionconstraints4, on = ['facilityname', 'periodname'], how = 'inner')
        df_productionconstraints5['ConstraintValueAdjusted'] = df_productionconstraints5[['constraintvalue_x', 'constraintvalue_y', 'quantity', 'PercentIssueVolume']].apply(lambda x: round(x[0]-min((x[0]/x[1])*(x[2]*x[3]*0.975),x[0]), 6), axis = 1)
        
        df_productionconstraints6 = df_productionconstraints5.rename(columns = {'ConstraintValueAdjusted':'production_constraint_value'})
        df_productionconstraints6['constraintvalue_x'] = df_productionconstraints6['constraintvalue_x'].fillna(0)
        df_productionconstraints6 = df_productionconstraints6.rename(columns = {'constraintvalue_x':'constraintvalue'})

        df_productionconstraints7 = df_productionconstraints[~df_productionconstraints['facilityname'].str.startswith('R_', na=False)]

        df_productionconstraints8 = df_productionconstraints7[df_productionconstraints7['facilityname'] == 'Depot']

        df_productionconstraints9 = df_productionconstraints7[~(df_productionconstraints7['facilityname'] == 'Depot')]

        df_productionconstraints10 = df_productionconstraints9[~(df_productionconstraints9['productname'] == 'RFU_NEW')]

       
       

        df_productionconstraints11 = df_productionconstraints5.groupby(['productname', 'periodname'])['constraintvalue_x', 'ConstraintValueAdjusted'].apply(sum)
        #df_productionconstraints11 = df_productionconstraints11.to_frame()
        df_productionconstraints11 = df_productionconstraints11.reset_index()

        df_productionconstraints11['WIPQuantity'] = df_productionconstraints11[['constraintvalue_x', 'ConstraintValueAdjusted']].apply(lambda x: math.floor(x[0] -x[1]), axis = 1)
    
        df_productionconstraints12 = df_productionconstraints11[df_productionconstraints11['productname'] == 'WIP']
    
        df_productionconstraints13 = pd.merge(df_productionconstraints8, df_productionconstraints12, on = 'periodname', how = 'inner')

        df_productionconstraints13['production constraint value adjusted'] = df_productionconstraints13[['constraintvalue_x', 'WIPQuantity']].apply(lambda x: x[0] - x[1], axis = 1)
        
       
        
        
        df_productionconstraints_final = pd.concat([df_productionconstraints10, df_final, df_productionconstraints6, df_productionconstraints13])


        df_productionconstraints_final['status'] = df_productionconstraints_final['original_status_field']
        df_productionconstraints_final['account_%s'%account] = df_productionconstraints_final['constraintvalue']
        for k in df_productionconstraints_final[[i for i in df_productionconstraints_final.columns.tolist() if 'account_' in i]].columns.tolist():
            print(df_productionconstraints_final[df_productionconstraints_final[k].notnull()])
        df_productionconstraints = df_productionconstraints_final[[i for i in df_productionconstraints_final.columns if i in df_productionconstraints or 'account_' in i]]
                        
        df_productionconstraints = df_productionconstraints.rename(columns = {i:i.lower() for i in df_productionconstraints.columns.tolist()})
        
    for col in df_productionconstraints.columns.tolist():
        if 'account_' in col or 'original_status_field' in col:
            engine.execute('alter table %s add column "%s" varchar(200)' %('productionconstraints',str(col)))
                
    engine.execute('delete from productionconstraints')
    df_productionconstraints.to_sql('productionconstraints', con=engine, if_exists='append',index=False)

    conn.close()
except Exception as e:
    conn.close()
    print(e)

END = dt.datetime.now()
print(END)
tot_sec = (END-START).total_seconds()

print('The process ended on: %s' %END)
print('It took %d minutes and %d seconds to finish the process' %(int(tot_sec//60), int(tot_sec - (tot_sec//60)*60)))