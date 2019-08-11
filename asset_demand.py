import psycopg2
from config import config
import pandas as pd
import numpy as np
from datetime import date

today = date.today()
date = today.strftime("%m.%d.%Y")

def connect(db):
    params = config.config(db)
    conn = psycopg2.connect(**params)
    return(conn)

def getData(db,sql):
    conn = None

    try:

        # Open database connection
        conn = connect(db)

        ## Open and read the file as a single buffer
        sqlFile  = open('SQL/' + sql,'r')

        df = pd.read_sql_query(sqlFile.read(),conn)

        ## close db conn and sql file
        sqlFile.close()
        #cur.close()
        return(df)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

##############################################################
# Gather ClosedStack asset data and solutioon to sku mapping #
##############################################################

##connect to the closedstack db and pull assets by solution aggregated by month as a df
asp_assets = getData('closedstackasp','demand_qty.sql')

##pull in solution mapping as a df
mapping = getData('cmis','solution_mapping.sql')

##########################################
# Merge asset data with solution mapping #
##########################################

## merge solution mapping with monthly solution counts
sku_demand_raw = pd.merge(asp_assets,mapping,left_on = ['name'],right_on = ['solution_name'])

sku_demand = sku_demand_raw.groupby(['sku','month'])['count'].agg({'count':[sum]})

sku_demand_raw = sku_demand_raw.drop(['solution_name','product'],axis=1)

####################################
# Fill in missing dates for values #
####################################

## reset the index in order to access the values
sku_demand = sku_demand.reset_index()

## cast the month and datetime datatype
sku_demand['month'] = pd.to_datetime(sku_demand['month'], format='%Y-%m-%d')

## capture the values needed to re-create a multi-index
start = sku_demand.month.min()
end   = sku_demand.month.max()
months = pd.date_range(start, end,freq=pd.DateOffset(months=1,day=1))
sku = sku_demand.sku.unique()

## re-create the index
sku_demand = sku_demand.set_index(['sku','month'])

## create a multi-index
mux = pd.MultiIndex.from_product([sku,months],
     names = ['sku','month'])

## re-index the dataframe
sku_demand = sku_demand.reindex(mux,fill_value=0)

## replace all missing values with 0
sku_demand = sku_demand.fillna(0)

sku_demand.columns = sku_demand.columns.droplevel(level=1)

################
# write to csv #
################

## Monthly demand by sku
sku_demand.to_csv(r'C:\Users\NB044705\OneDrive - Cerner Corporation\development\output\asset_mapping.csv')

## Monthly demand by solution
sku_demand_raw.to_csv(r'C:\Users\NB044705\OneDrive - Cerner Corporation\development\output\asp_demand.csv',index=False)
