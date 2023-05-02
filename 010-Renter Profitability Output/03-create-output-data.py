import datetime as dt
import sqlalchemy as sal
#from sqlalchemy import create_engine
import pandas as pd
from optilogic import pioneer

####################### BEGIN USER INPUTS #######################
USER_NAME = 'graham.billey'
APP_KEY = 'op_NWQ3YjQ0NjktNTBjOC00M2JkLWE4NWEtNjM1NDBmODA5ODEw'
DB_NAME = 'PECO 2023-05 SOIP Opt (Cost to Serve)' # Cosmic Frog Model Name


######################## END USER INPUTS ########################

# Start time of the process
START = dt.datetime.now()
print('The process started on: %s' %START)

#%%#################################################################################################

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
                   'facilities']
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
tc_i = ols[['scenarioname', 'departingperiodname', 'destinationname', 'transportationcost']].copy()
tc_i = tc_i[(tc_i['destinationname'].str.contains('I_')) & 
            (tc_i['departingperiodname'].str[-2:].astype(int) <= 12)]
tc_i = tc_i.groupby(['scenarioname'])['transportationcost'].sum()
tc_i.rename('Transportation Cost - Issues', inplace=True)

# Transportation Cost - Returns
tc_r = ols[['scenarioname', 'departingperiodname', 'originname', 'transportationcost']].copy()
tc_r = tc_r[(tc_r['originname'].str.contains('R_')) & 
            (tc_r['departingperiodname'].str[-2:].astype(int) <= 12)]
tc_r = tc_r.groupby(['scenarioname'])['transportationcost'].sum()
tc_r.rename('Transportation Cost - Returns', inplace=True)

# Transportation Cost - Transfers
tc_t = ols[['scenarioname', 'departingperiodname', 'originname', 'destinationname', 'transportationcost']].copy()
tc_t = tc_t[~(tc_t['destinationname'].str.contains('I_')) & 
            ~(tc_t['originname'].str.contains('R_')) & 
            (tc_t['departingperiodname'].str[-2:].astype(int) <= 12)]
tc_t = tc_t.groupby(['scenarioname'])['transportationcost'].sum()
tc_t.rename('Transportation Cost - Transfers', inplace=True)

# Total Fixed Cost
tfc = ofs[['scenarioname', 'periodname', 'operatingcost']].copy()
tfc = tfc[tfc['periodname'].str[-2:].astype(int) <= 12]
tfc = tfc.groupby(['scenarioname'])['operatingcost'].sum()
tfc.rename('Total Fixed Cost', inplace=True)

# Total Painting Cost
mfrs = list(fac[fac['depottype']=='Manufacturing']['facilityname'])
tpc = ols[['scenarioname', 'departingperiodname', 'originname', 'sourcingcost']].copy()
tpc = tpc[~(tpc['originname'].isin(mfrs)) &
          (tpc['departingperiodname'].str[-2:].astype(int) <= 12)]
tpc = tpc.groupby(['scenarioname'])['sourcingcost'].sum()
tpc.rename('Total Painting Cost', inplace=True)

# Total Depot Handling Cost
thc = ows[['scenarioname', 'periodname', 'inboundhandlingcost', 'outboundhandlingcost']].copy()
thc['totalhandlingcost'] = thc['inboundhandlingcost'] + thc['outboundhandlingcost']
thc = thc[thc['periodname'].str[-2:].astype(int) <= 12]
thc = thc.groupby(['scenarioname'])['totalhandlingcost'].sum()
thc.rename('Total Depot Handling Cost', inplace=True)

# Total Inventory Carrying Cost
tic = ois[['scenarioname', 'periodname', 'totalinventorycost']].copy()
tic = tic[tic['periodname'].str[-2:].astype(int) <= 12]
tic = tic.groupby(['scenarioname'])['totalinventorycost'].sum()
tic.rename('Total Inventory Carrying Cost', inplace=True)

# Total Repair Cost
trc = ops[['scenarioname', 'startingperiodname', 'bomname', 'productioncost']]
trc = trc[(trc['bomname'] == 'BOM_RFU_REPAIR') &
          (trc['startingperiodname'].str[-2:].astype(int) <= 12)]
trc = trc.groupby(['scenarioname'])['productioncost'].sum()
trc.rename('Total Repair Cost', inplace=True)

# Total Depot Cost
tdc = tfc + tpc + thc + trc
tdc.rename('Total Depot Cost', inplace=True)

# Total Variable Cost CPI
tvc_cpi = (tpc + thc + trc) / issues
tvc_cpi.rename('Total Variable Cost CPI', inplace=True)

# Total Fixed Cost CPI
tfc_cpi = tfc / issues
tfc_cpi.rename('Total Fixed Cost CPI', inplace=True)



#%%#################################################################################################
print('Combining cost data into one DataFrame.')

data = pd.concat([issues, returns, r_i, tc_i, tc_r, tc_t, tfc, tpc, thc, tic, trc, tdc, tvc_cpi, tfc_cpi], axis=1)
#data.to_excel('data_test_2023-05-02.xlsx')


#%%#################################################################################################

print('Calculating differences between baseline and scenarios with removed accounts.')

baseline   = 'SOIP (12 Month) (Dedicated Overrides MultiSourcing)'
optimal    = 'SOIP Optimize (12 Month)'
less_accts = [scenario for scenario in data.index if 'Less' in scenario]

# For each account in less_accts, we want to subtract that row from baseline and give that
# new row the index f"baseline minus {account}".

subtracted_accts = dict()

for acct in less_accts:
    acct_number = acct[-5:]
    # Compare to Baseline
    delta = data.loc[baseline] - data.loc[acct]
    delta.name = f'Baseline minus {acct_number}'
    subtracted_accts[delta.name] = pd.DataFrame(delta).T
    
    # Compare to Optimal (with reassignments allowed)
    delta = data.loc[optimal] - data.loc[acct]
    delta.name = f'Reassignment minus {acct_number}'
    subtracted_accts[delta.name] = pd.DataFrame(delta).T

delta_df = pd.concat([df for df in subtracted_accts.values()])









