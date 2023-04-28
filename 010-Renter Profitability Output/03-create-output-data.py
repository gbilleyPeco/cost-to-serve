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
conn = engine.connect()

# List of all Cosmic Frog Model tables
db_tables = engine.table_names()
tables_we_want  = ['optimizationnetworksummary', 
                   'optimizationshipmentsummary',
                   'optimizationfacilitysummary']
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

#%%#################################################################################################

# All columns in Optimization Network Summary
ons = data_dict['optimizationnetworksummary']   # Note: This likely can't be used since we only want Periods 1-12.
ons.columns
# =============================================================================
#       ['id', 'scenarioname', 'neoversion', 'dartversion', 'runstarttime',
#        'totalruntime', 'solvetime', 'optimizationgappercentage',
#        'totalsupplychaincost', 'totalrevenue', 'totalprofit',
#        'totalserveddemandquantity', 'totalunserveddemandquantity',
#        'quantityuom', 'totalserveddemandvolume', 'totalunserveddemandvolume',
#        'volumeuom', 'totalserveddemandweight', 'totalunserveddemandweight',
#        'weightuom', 'totaltransportationcost', 'totalshipmentcost',
#        'totalintransitholdingcost', 'totaldutycost', 'totalproductioncost',
#        'totalprebuildholdingcost', 'totalturnestimatedholdingcost',
#        'totalstoragecost', 'totalsourcingcost', 'totalfixedoperatingcost',
#        'totalfixedstartupcost', 'totalfixedclosingcost',
#        'totalinboundhandlingcost', 'totaloutboundhandlingcost',
#        'totalprocesscost', 'totaluserdefinedcost', 'optiriskscore']
# =============================================================================

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



# ======================================================================================================
#                        Pseudo-SQL diagram of where to get data
#               Excel                       :                       Cosmic Frog
# scenario_name                             :   *.scenarioname
# model_name                                :   N/A
# issue volume                              :   oss.shipmentsize where dest like 'I_' and period <= 12
# return volume                             :   oss.shipmentsize where orig like 'R_' and period <= 12
# transportation cost - issue               :   oss.shipmentcost where dest like 'I_' and period <= 12
# transportation cost - return              :   oss.shipmentcost where orig like 'R_' and period <= 12
# baseline total fixed cost                 :   
# baseline total painting cost              :   
# baseline total depot handling cost        :   
# baseline total inv carrying cost          :   
# baseline total repairs                    :   
# baseline total depot cost                 :   
# baseline total depot cost variable CPI    :   
# baseline total depot cost fixed CPI       :   
# ======================================================================================================



columns_we_want = []





