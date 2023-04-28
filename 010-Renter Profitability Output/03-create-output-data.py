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





# =============================================================================
# for i in db_tables:
#     with engine.connect() as conn:
#         trans = conn.begin()
#         if i == 'customers':
#             print('Reading customers table.')
#             df_customers = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
#             if 'id' in df_customers.columns:
#                 del df_customers['id']
#         elif i == 'periods':
#             print('Reading periods table.')
#             df_periods = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
#             if 'id' in df_periods.columns:
#                 del df_periods['id']
#         elif i == 'customerdemand':
#             print('Reading customerdemand table.')
#             df_demand = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
#             if 'id' in df_demand.columns:
#                 del df_demand['id']
#         elif i == 'inventorypolicies':
#             print('Reading inventorypolicies table.')
#             df_inventorypolicies = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
#             if 'id' in df_inventorypolicies.columns:
#                 del df_inventorypolicies['id']
#         elif i == 'productionconstraints':
#             print('Reading productionconstraints table.')
#             df_productionconstraints = pd.read_sql_query("SELECT * FROM %s"%i, con=conn)
#             if 'id' in df_productionconstraints.columns:
#                 del df_productionconstraints['id']
#         trans.commit()
# =============================================================================

#%%#################################################################################################

tables_we_want  = []
columns_we_want = []





