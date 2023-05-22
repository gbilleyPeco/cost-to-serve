import sqlalchemy as sal
import pandas as pd
import warnings
from optilogic import pioneer

####################### BEGIN USER INPUTS #######################
USER_NAME = 'graham.billey'
APP_KEY = 'op_NWQ3YjQ0NjktNTBjOC00M2JkLWE4NWEtNjM1NDBmODA5ODEw'
DB_NAME = 'PECO 2023-05 SOIP Opt (Cost to Serve)' # Cosmic Frog Model Name
######################## END USER INPUTS ########################

#%%#################################################################################################

with warnings.catch_warnings():
    warnings.simplefilter("ignore")     # Ignore the Cosmic Frog API warning.

    # Code that makes connection to the Cosmic Frog database specified at the user input above
    api = pioneer.Api(auth_legacy = False, un=USER_NAME, appkey=APP_KEY)
    connection_str = api.sql_connection_info(DB_NAME)
    connection_string = 'postgresql://'+connection_str['raw']['user']+':'+connection_str['raw']['password']+'@'+connection_str['raw']['host']+':'+str(connection_str['raw']['port'])+'/'+connection_str['raw']['dbname']+'?sslmode=require'
    engine = sal.create_engine(connection_string)
    insp = sal.inspect(engine)
    conn = engine.connect()

# List of all Cosmic Frog Model tables
db_tables = insp.get_table_names()

tables_we_want  = ['optimizationshipmentsummary',
                   'optimizationfacilitysummary',
                   'optimizationflowsummary',
                   'optimizationwarehousingsummary',
                   'optimizationinventorysummary',
                   'optimizationproductionsummary',
                   'facilities',
                   'customers',
                   'productionconstraints']
data_dict = {}

for i in db_tables:
    with engine.connect() as conn:
        trans = conn.begin()
        if i in tables_we_want:
            print(f'Reading table: {i}')
            data = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
            if 'id' in data.columns:
                del data['id']
            data_dict[i] = data
        trans.commit()
del data

#%%#################################################################################################

# All columns in Optimization Shipment Summary
oss = data_dict['optimizationshipmentsummary']
oss.columns
# =============================================================================
#       ['id', 'scenarioname', 'periodname', 'originname', 'destinationname',
#        'productgroupname', 'modename', 'shipments', 'shipmentcost',
#        'shipmentsize', 'shipmentsizeuom', 'originlatitude', 'originlongitude',
#        'destinationlatitude', 'destinationlongitude']
# =============================================================================

# All columns in Optimization Facility Summary
ofs = data_dict['optimizationfacilitysummary']
ofs.columns
# =============================================================================
#       ['scenarioname', 'periodname', 'facilityname', 'initialstate',
#        'initialstatus', 'optimizedstatus', 'totalfacilitycost',
#        'operatingcost', 'startupcost', 'closingcost',
#        'totalinboundhandlingcost', 'totaloutboundhandlingcost',
#        'totalprebuildholdingcost', 'totalturnestimatedholdingcost',
#        'totalstoragecost', 'totalintransitholdingcost', 'totalproductioncost',
#        'totalprocesscost', 'totalinboundquantity', 'totaloutboundquantity',
#        'totalinventoryquantity', 'totalproductionquantity', 'quantityuom',
#        'totalinboundvolume', 'totaloutboundvolume', 'totalinventoryvolume',
#        'totalproductionvolume', 'volumeuom', 'totalinboundweight',
#        'totaloutboundweight', 'totalinventoryweight', 'totalproductionweight',
#        'weightuom', 'throughputcapacity', 'throughputcapacityuom',
#        'throughpututilization', 'storagecapacity', 'storagecapacityuom',
#        'storageutilization', 'riskscore', 'concentrationrisk',
#        'sourcecountrisk', 'capacityrisk', 'storagecapacityrisk',
#        'throughputcapacityrisk', 'workcentercapacityrisk', 'geographicrisk',
#        'latitude', 'longitude']
# =============================================================================

# All columns in Optimization Flow Summary
ols = data_dict['optimizationflowsummary']
ols.columns
# =============================================================================
#       ['scenarioname', 'departingperiodname', 'arrivingperiodname', 'flowtype',
#        'originname', 'destinationname', 'productname', 'modename',
#        'flowquantity', 'flowquantityuom', 'flowweight', 'flowweightuom',
#        'flowvolume', 'flowvolumeuom', 'sourcingcost', 'transportationcost',
#        'shipmentcost', 'dutycost', 'intransitholdingcost', 'totalcost',
#        'transportdistance', 'transportdistanceuom', 'transporttime',
#        'transporttimeuom', 'transporttimerisk', 'timetoimportrisk',
#        'timetoexportrisk', 'originlatitude', 'originlongitude',
#        'destinationlatitude', 'destinationlongitude']
# =============================================================================

# All columns in Optimization Warehousing Summary
ows = data_dict['optimizationwarehousingsummary']
ows.columns
# =============================================================================
#       ['scenarioname', 'periodname', 'facilityname', 'productname',
#        'inboundhandlingcost', 'stockingcost', 'destockingcost',
#        'outboundhandlingcost', 'inboundquantity', 'stockedquantity',
#        'destockedquantity', 'outboundquantity', 'quantityuom', 'inboundvolume',
#        'stockedvolume', 'destockedvolume', 'outboundvolume', 'volumeuom',
#        'inboundweight', 'stockedweight', 'destockedweight', 'outboundweight',
#        'weightuom', 'latitude', 'longitude']
# =============================================================================

# All columns in Optimization Inventory Summary
ois = data_dict['optimizationinventorysummary']
ois.columns
# =============================================================================
#       ['scenarioname', 'periodname', 'facilityname', 'productname',
#        'averageprebuildinventoryquantity', 'startingprebuildinventoryquantity',
#        'endingprebuildinventoryquantity', 'maxturnestimatedinventoryquantity',
#        'inventoryquantityuom', 'averageprebuildinventoryvolume',
#        'startingprebuildinventoryvolume', 'endingprebuildinventoryvolume',
#        'maxturnestimatedinventoryvolume', 'inventoryvolumeuom',
#        'averageprebuildinventoryweight', 'startingprebuildinventoryweight',
#        'endingprebuildinventoryweight', 'maxturnestimatedinventoryweight',
#        'inventoryweightuom', 'totalinventorycost', 'prebuildholdingcost',
#        'turnestimatedholdingcost', 'storagecost', 'latitude', 'longitude']
# =============================================================================

# All columns in Optimization Production Summary
ops = data_dict['optimizationproductionsummary']
ops.columns
# =============================================================================
#       ['scenarioname', 'startingperiodname', 'completionperiodname',
#        'facilityname', 'productname', 'bomname', 'processname',
#        'productionquantity', 'productionquantityuom', 'productionvolume',
#        'productionvolumeuom', 'productionweight', 'productionweightuom',
#        'productioncost', 'processcost', 'productiontime', 'productiontimeuom',
#        'latitude', 'longitude']
# =============================================================================

fac = data_dict['facilities']
fac.columns
# =============================================================================
#       ['facilityname', 'status', 'facilitystatus', 'initialstate',
#        'organization', 'address', 'city', 'region', 'postalcode', 'country',
#        'latitude', 'longitude', 'fixedstartupcost', 'fixedoperatingcost',
#        'fixedclosingcost', 'storagecapacity', 'storagecapacityuom',
#        'throughputcapacity', 'throughputcapacityuom', 'singlesource',
#        'singlesourceorders', 'singlesourcelineitems', 'allowbackorders',
#        'backordertimelimit', 'backordertimeuom', 'allowpartialfillorders',
#        'allowpartialfilllineitems', 'operatingschedule', 'operatingcalendar',
#        'notes', 'loccode', 'locdescription', 'depottype', 'region.1', 'closed',
#        'returnqty', 'avgloadsz', 'locationtype', 'lower48_can_mex',
#        'pecoregion', 'corpcode', 'corpname', 'insoipmodel',
#        'renterreturnlocation', 'passone', 'pecosubregion', 'liveload',
#        'liveloadrank', 'zone', 'heat_treatment_rqmt', 'transservicetype',
#        'new_pallet', 'queuepriority']
# =============================================================================

cus = data_dict['customers']
cus.columns
# =============================================================================
#       ['customername', 'status', 'address', 'city', 'region', 'postalcode',
#        'country', 'latitude', 'longitude', 'singlesource',
#        'singlesourceorders', 'singlesourcelineitems', 'allowbackorders',
#        'backordertimelimit', 'backordertimeuom', 'allowpartialfillorders',
#        'allowpartialfilllineitems', 'allowdirectship', 'notes', 'region.1',
#        'issueqty', 'avgloadsz', 'loccode', 'locdescription', 'lower48_can_mex',
#        'soipquantity', 'pecoregion', 'corpcode', 'corpname', 'insoip',
#        'pecosubregion', 'liveload', 'liveloadrank', 'zone',
#        'heat_treatment_rqmt', 'new_pallet_rqmt', 'depottype',
#        'transservicetype', 'queuepriority']
# =============================================================================

prd = data_dict['productionconstraints']
prd.columns
# =============================================================================
#       ['facilityname', 'facilitynamegroupbehavior', 'productname',
#        'productnamegroupbehavior', 'periodname', 'periodnamegroupbehavior',
#        'bomname', 'bomnamegroupbehavior', 'processname',
#        'processnamegroupbehavior', 'constrainttype', 'constraintvalue',
#        'constraintvalueuom', 'status', 'notes', 'soipquantity',
#        'conversionnotes', 'original_status_field', 'account_81384',
#        'account_81399', 'account_85557', 'account_15191', 'account_14285',
#        'account_14739', 'account_14864', 'account_14904', 'account_14994',
#        'account_15011', 'account_15738', 'account_80290', 'account_80293',
#        'account_80500', 'account_81377', 'account_81398', 'account_81688',
#        'account_82408', 'account_86410', 'account_87025', 'account_88008',
#        'account_91572', 'account_91901']
# =============================================================================

# ==================================================================================================
#                        Pseudo-SQL diagram of where to get data
#               Excel                       :                       Cosmic Frog
# scenario_name                             :   *.scenarioname
# model_name                                :   N/A
# issue volume                              :   ols.flowquantity where dest like 'I_' and period <= 12
# return volume                             :   ols.flowquantity where orig like 'R_' and period <= 12
# transportation cost - issue               :   ols.transportationcost where dest like 'I_' and period <= 12
# transportation cost - return              :   ols.transportationcost where orig like 'R_' and period <= 12
# transportation cost - transfer            :   ols.transportationcost where [not issue or return] and period <= 12
# total fixed cost                       A  :   ofs.operatingcost where period <= 12
# total painting cost                    B  :   ols.sourcingcost where [source is not a manufacturer] and departingperiodname <= 12
# total depot handling cost              C  :   ows.inboundhandlingcost + ows.outboundhandlingcost where period <= 12
# total inv carrying cost                D  :   ois.totalinventorycost where period <= 12
# total repair cost                      E  :   ops.productioncost where bomname = 'BOM_RFU_REPAIR' and startingperiod <= 12
# total depot cost                       F  :   A+B+C+E (Not D)
# total depot cost variable CPI          G  :   B+C+E / issues
# total depot cost fixed CPI             H  :   A / Issues
# ==================================================================================================

#%%#################################################################################################
print('Calculating quantities and costs across all Opt scenarios.')

# Issues
issues = ols[['scenarioname', 'departingperiodname', 'destinationname', 'flowquantity']].copy()
issues = issues[(issues['destinationname'].str.contains('I_')) & (issues['departingperiodname'].str[-2:].astype(int) <= 12)]
issues = issues.groupby(['scenarioname'])['flowquantity'].sum()
issues.rename('Issues', inplace=True)


# Returns
returns = ols[['scenarioname', 'departingperiodname', 'originname', 'flowquantity']].copy()
returns = returns[(returns['originname'].str.contains('R_')) & (returns['departingperiodname'].str[-2:].astype(int) <= 12)]
returns = returns.groupby(['scenarioname'])['flowquantity'].sum()
returns.rename('Returns', inplace=True)

# Network R:I
r_i = returns / issues
r_i.rename('Network R:I', inplace=True)

# Transportation Cost - Issues
tc_i = ols[['scenarioname', 'departingperiodname', 'destinationname', 'shipmentcost', 'sourcingcost', 'transportationcost']].copy()
tc_i['newcost'] = tc_i['shipmentcost'] + tc_i['sourcingcost'] + tc_i['transportationcost']
tc_i = tc_i[(tc_i['destinationname'].str.contains('I_')) & 
            (tc_i['departingperiodname'].str[-2:].astype(int) <= 12)]
tc_i = tc_i.groupby(['scenarioname'])['newcost'].sum()
tc_i.rename('Transportation Cost - Issues', inplace=True)

# Transportation Cost - Returns
tc_r = ols[['scenarioname', 'departingperiodname', 'originname', 'shipmentcost', 'sourcingcost', 'transportationcost']].copy()
tc_r['newcost'] = tc_r['shipmentcost'] + tc_r['sourcingcost'] + tc_r['transportationcost']
tc_r = tc_r[(tc_r['originname'].str.contains('R_')) & 
            (tc_r['departingperiodname'].str[-2:].astype(int) <= 12)]
tc_r = tc_r.groupby(['scenarioname'])['newcost'].sum()
tc_r.rename('Transportation Cost - Returns', inplace=True)

# Transportation Cost - Transfers
tc_t = ols[['scenarioname', 'departingperiodname', 'originname', 'destinationname', 'shipmentcost', 'sourcingcost', 'transportationcost']].copy()
tc_t['newcost'] = tc_t['shipmentcost'] + tc_t['sourcingcost'] + tc_t['transportationcost']
tc_t = tc_t[~(tc_t['destinationname'].str.contains('I_')) & 
            ~(tc_t['originname'].str.contains('R_')) & 
            (tc_t['departingperiodname'].str[-2:].astype(int) <= 12)]
tc_t = tc_t.groupby(['scenarioname'])['newcost'].sum()
tc_t.rename('Transportation Cost - Transfers', inplace=True)

# Depot Cost - Fixed
tfc = ofs[['scenarioname', 'periodname', 'operatingcost']].copy()
tfc = tfc[tfc['periodname'].str[-2:].astype(int) <= 12]
tfc = tfc.groupby(['scenarioname'])['operatingcost'].sum()
tfc.rename('Depot Cost - Fixed', inplace=True)

# Depot Cost - Painting
mfrs = list(fac[fac['depottype']=='Manufacturing']['facilityname'])
tpc = ols[['scenarioname', 'departingperiodname', 'originname', 'sourcingcost']].copy()
tpc = tpc[~(tpc['originname'].isin(mfrs)) &
          (tpc['departingperiodname'].str[-2:].astype(int) <= 12)]
tpc = tpc.groupby(['scenarioname'])['sourcingcost'].sum()
tpc.rename('Depot Cost - Painting', inplace=True)

# Depot Cost - Handling
thc = ows[['scenarioname', 'periodname', 'inboundhandlingcost', 'outboundhandlingcost']].copy()
thc['totalhandlingcost'] = thc['inboundhandlingcost'] + thc['outboundhandlingcost']
thc = thc[thc['periodname'].str[-2:].astype(int) <= 12]
thc = thc.groupby(['scenarioname'])['totalhandlingcost'].sum()
thc.rename('Depot Cost - Handling', inplace=True)

# Depot Cost - Inventory Carrying (Not Included in Total)
tic = ois[['scenarioname', 'periodname', 'totalinventorycost']].copy()
tic = tic[tic['periodname'].str[-2:].astype(int) <= 12]
tic = tic.groupby(['scenarioname'])['totalinventorycost'].sum()
tic.rename('Depot Cost - Inventory Carrying (Not Included in Total)', inplace=True)

# Depot Cost - Repair
trc = ops[['scenarioname', 'startingperiodname', 'bomname', 'productioncost']]
trc = trc[(trc['bomname'] == 'BOM_RFU_REPAIR') &
          (trc['startingperiodname'].str[-2:].astype(int) <= 12)]
trc = trc.groupby(['scenarioname'])['productioncost'].sum()
trc.rename('Depot Cost - Repair', inplace=True)

#Total Transportation Cost
ttc = tc_i + tc_r + tc_t
ttc.rename('Total Transportation Cost: Issues + Returns + Transfers', inplace=True)

# Total Depot Cost
tdc = tfc + tpc + thc + trc
tdc.rename('Total Depot Cost: Fixed + Painting + Handling + Repair', inplace=True)

# Total Variable Cost CPI
tvc_cpi = (tpc + thc + trc) / issues
tvc_cpi.rename('Total Variable Cost CPI', inplace=True)

# Total Fixed Cost CPI
tfc_cpi = tfc / issues
tfc_cpi.rename('Total Fixed Cost CPI', inplace=True)

# Total CPI
t_cpi = tvc_cpi + tfc_cpi
t_cpi.rename('Total Cost per Issue', inplace=True)

#%%#################################################################################################
print('Creating the Scenario DataFrame.')

scenario_data = pd.concat([issues, returns, r_i, tc_i, tc_r, tc_t, ttc, tfc, tpc, thc, tic, trc, tdc, tvc_cpi, tfc_cpi, t_cpi], axis=1)

#%%#################################################################################################
print('Calculating differences between baseline and scenarios with removed accounts.')

baseline   = 'SOIP (12 Month) (Dedicated Overrides MultiSourcing)'
optimal    = 'SOIP Optimize (12 Month)'
less_accts = [scenario for scenario in scenario_data.index if 'Less' in scenario]

subtracted_dfs = dict()

for acct in less_accts:
    acct_number = acct[-5:]
    
    # Compare to Baseline
    delta = scenario_data.loc[acct] - scenario_data.loc[baseline]
    delta.name = f'Removing {acct_number} - Baseline'
    subtracted_dfs[delta.name] = pd.DataFrame(delta).T
    
    # Compare to Optimal (with reassignments allowed)
    delta = scenario_data.loc[acct] - scenario_data.loc[optimal]
    delta.name = f'Removing {acct_number} - Optimal'
    subtracted_dfs[delta.name] = pd.DataFrame(delta).T


# Combine the "subtracted" rows with the original dataset.
delta_df = pd.concat([df for df in subtracted_dfs.values()])
scenario_data_final = pd.concat([scenario_data, delta_df])

print('Exporting the Scenario Dataframe.')
# Export
scenario_data_final.to_excel('010-Renter Profitability Output/troubleshooting/scenario_data_2023-05-22.xlsx')


#%%#################################################################################################

# Calculate Issues and Returns costs for each customer based on the 
# OptimizationFlowSummary (Issue Count and Issues Costs) and 
# ProductionConstraints (Return Count) and OptimizationFlowSummary (Return Costs) tables.

optimal_scenario = 'SOIP Optimize (12 Month)'

# Get all customer corp codes and names.
corp_codes = [i[-5:] for i in less_accts]
customers = cus[['loccode', 'customername', 'corpcode', 'corpname']].drop_duplicates()
customers['Customer'] = customers['corpcode'] + ' - ' + customers['corpname']
customers = customers[customers['corpcode'].isin(corp_codes)]

########################## Current Network - Transportation Costs, Issues ##########################
tc_i_cn = ols[['scenarioname', 'departingperiodname', 'destinationname', 
               'shipmentcost', 'sourcingcost', 'transportationcost']].copy()

# Filter for only the rows we want.
tc_i_cn = tc_i_cn[(tc_i_cn['destinationname'].str.contains('I_')) &                 # Issues only.
                  (tc_i_cn['departingperiodname'].str[-2:].astype(int) <= 12) &     # 12 Months only.
                  (tc_i_cn['scenarioname'] == optimal_scenario)]                    # Scenario with the customer in the network.

# Calculate the total issue cost for each row.
tc_i_cn['Transportation Cost - Issues'] = tc_i_cn['shipmentcost'] + tc_i_cn['sourcingcost'] + tc_i_cn['transportationcost']

# Join with the customers table so we can group by corp code.
tc_i_cn = tc_i_cn.merge(customers, how='inner', left_on='destinationname', right_on='customername')

# Group by Customer (Corp Code + Corp Name)
tc_i_cn = tc_i_cn.groupby(['corpcode'])['Transportation Cost - Issues'].sum()

########################## Current Network - Transportation Costs, Returns #########################

# Get all rows in the Production Constraints table that correspond to returns.
prd_returns = prd[prd['notes'] == 'Returns_SOIP']

return_locations = pd.DataFrame()

for acct in corp_codes:
    
    # Get the adjusted constraint value column for the given acct.
    # NOTE: Ignore 'productname' and 'periodname'. Treat WIP and RFU the same. Aggregate across all periods.
    acct_colname = 'account_'+acct    
    temp_df = prd_returns[['facilityname', 'constraintvalue', acct_colname]].copy()
    
    # Calculate the returns removed due to that given customer.
    temp_df['returns'] = temp_df['constraintvalue'].astype(float) - temp_df[acct_colname].astype(float)
    
    # Add a column for the corp code, to group by later.
    temp_df['corp_code'] = acct
    
    # Union all return dataframes together.
    return_locations = pd.concat([return_locations, temp_df[['facilityname', 'returns', 'corp_code']]])

# Groupby ['facilityname', 'corp_code'], sum('returns')
return_locations_grouped = return_locations.groupby(['facilityname', 'corp_code']).sum('returns').reset_index()


# Get the transportation cost per pallet for all returns.
# Filter only the rows we want.
return_costs = ols[(ols['scenarioname'] == optimal_scenario) &  
                   (ols['originname'].str.contains('R_')) &    
                   (ols['departingperiodname'].str[-2:].astype(int) <= 12)].copy()

# Grab only the columns we care about.
return_costs = return_costs[['scenarioname', 'departingperiodname', 'originname', 'destinationname',
                             'flowquantity', 'shipmentcost', 'transportationcost']]

# Get the total transportation cost.
return_costs['total transportation cost'] = return_costs['shipmentcost'] + return_costs['transportationcost']

# Group by return location.
return_costs_grouped = return_costs.groupby(['originname']).sum(['flowquantity', 'total transportation cost']).reset_index()

# Calculate cost per pallet.
return_costs_grouped['Return Cost per Pallet'] = return_costs_grouped['total transportation cost'] / return_costs_grouped['flowquantity']

# Join the return locations with their cost per pallet. 
return_location_costs = return_locations_grouped.merge(return_costs_grouped,
                                                       how='outer', 
                                                       left_on='facilityname',
                                                       right_on='originname')

# Calculate total return cost for each row in the new dataframe.
return_location_costs['Return Cost'] = return_location_costs['returns'] * return_location_costs['Return Cost per Pallet']

# Keep only the columns we want.
return_location_costs = return_location_costs[['facilityname', 'corp_code', 'returns', 
                                               'Return Cost per Pallet', 'Return Cost']]

# Join with corporate code names.
return_location_costs = return_location_costs.merge(customers[['corpcode', 'Customer']].drop_duplicates(), 
                                                    how='left',
                                                    left_on='corp_code',
                                                    right_on='corpcode')

# Group by Customer (Corp Code + Corp Name)
tc_r_cn = return_location_costs[['corpcode', 'Return Cost']].groupby(['corpcode']).sum()
tc_r_cn.rename(columns={'Return Cost':'Transportation Cost - Returns'}, inplace=True)

#%%#################################################################################################

# Create the "Account" DataFrame. 
# Each row should be a corporate account.
# The following are the columns:
#   Issues
#   Returns
#   R:I
#   In-Network Cost-to-Issue            (Calculated directly) as tc_i_cn
#   In-Network Cost-to-Return           (Calculated directly) as tc_r_cn
#   In-Network Cost-to-Transfer         (Calculated indirectly)
#   Out-of-Network Cost-to-Issue        (Calculated directly)
#   Out-of-Network Cost-to-Return       (Calculated directly)
#   Out-of-Network Cost-to-Transfer     (Calculated directly)

# Create the customer dataframe
customer_data = pd.DataFrame()

for corp in corp_codes:
    scenario_data_index = f'Removing {corp} - Optimal'
    
    # Account Issues
    corp_issues = -1*scenario_data_final.loc[scenario_data_index]['Issues']
    
    # Account Returns
    corp_returns = -1*scenario_data_final.loc[scenario_data_index]['Returns']
    
    # Account R:I
    corp_ri = corp_returns / corp_issues
    
    # Out-of-Network Transportation Cost - Issues
    oon_tc_i = -1*scenario_data_final.loc[scenario_data_index]['Transportation Cost - Issues']
    
    # Out-of-Network Transportation Cost - Returns
    oon_tc_r = -1*scenario_data_final.loc[scenario_data_index]['Transportation Cost - Returns']
    
    # Out-of-Network Transportation Cost - Transfers
    oon_tc_t = -1*scenario_data_final.loc[scenario_data_index]['Transportation Cost - Transfers']
    
    new_row = pd.DataFrame(data=[[corp_issues, corp_returns, corp_ri, oon_tc_i, oon_tc_r, oon_tc_t]],
                           columns=['Account Issues',
                                    'Account Returns',
                                    'Account R:I',
                                    'Optimized Network Transportation Cost - Issues',
                                    'Optimized Network Transportation Cost - Returns',
                                    'Optimized Network Transportation Cost - Transfers'],
                           index=[corp])
    customer_data = pd.concat([customer_data, new_row])

# In-Network Transportation Cost - Issues
# In-Network Transportation Cost - Returns
customer_data = pd.concat([customer_data, tc_i_cn, tc_r_cn], axis=1)
customer_data.rename(columns={'Transportation Cost - Issues':'Current Network Transportation Cost - Issues',
                              'Transportation Cost - Returns':'Current Network Transportation Cost - Returns'},
                     inplace=True)

# In-Network Transportation Cost - Transfers.
# Note - doing the math this way so it's easier to read.
A = customer_data['Current Network Transportation Cost - Issues']
B = customer_data['Current Network Transportation Cost - Returns']
C = customer_data['Optimized Network Transportation Cost - Issues']
D = customer_data['Optimized Network Transportation Cost - Returns']
E = customer_data['Optimized Network Transportation Cost - Transfers']

customer_data['Current Network Transportation Cost - Transfers'] = C+D+E - (A+B)

# Create "Cost per Issue" columns.
i = customer_data['Account Issues']
customer_data['Current Network Transportation Cost - Issues - CPI'] = customer_data['Current Network Transportation Cost - Issues'] / i
customer_data['Current Network Transportation Cost - Returns - CPI'] = customer_data['Current Network Transportation Cost - Returns'] / i
customer_data['Current Network Transportation Cost - Transfers - CPI'] = customer_data['Current Network Transportation Cost - Transfers'] / i
customer_data['Optimized Network Transportation Cost - Issues - CPI'] = customer_data['Optimized Network Transportation Cost - Issues'] / i
customer_data['Optimized Network Transportation Cost - Returns - CPI'] = customer_data['Optimized Network Transportation Cost - Returns'] / i
customer_data['Optimized Network Transportation Cost - Transfers - CPI'] = customer_data['Optimized Network Transportation Cost - Transfers'] / i

customer_data = customer_data.merge(customers[['corpcode', 'Customer']].drop_duplicates(),
                           left_index=True,
                           right_on='corpcode')
customer_data.set_index(['corpcode', 'Customer'], inplace=True)

customer_data.to_excel('010-Renter Profitability Output/troubleshooting/customer_data_2023-05-22.xlsx')

#%%#################################################################################################

'''
Add:
    Fixed, variable, total depot costs (as differenve between optimized with customer and optimized without customer)
    total transportation cost
    

'''















