# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 14:19:52 2019

@author: KatieSi
"""


##############################################################################
### Import Packages
##############################################################################

import numpy as np
import pandas as pd
import pdsql
from datetime import datetime, timedelta, date
import networkx as nx
import os


##############################################################################
### Set Variables
##############################################################################

ReportName= 'WAP Allocation Calculator'
RunDate = str(date.today())

EarliestTermination = '2018-07-01'
LatestGivenEffect = '2019-07-01'

ConsentStatus = ['Terminated - Replaced',
                           'Issued - Active',
                           'Issued - s124 Continuance'] 

output_path = r"D:\\Implementation Support\\Python Scripts\\scripts\\Export\\"


##############################################################################
### Import Data
##############################################################################

##############################################################################
### Import Base Consent Information

ConsentDetailsCol = [
        'B1_ALT_ID',
        'B1_APPL_STATUS',
        'fmDate',
        'toDate',
        'HolderAddressFullName',
        'HolderEcanID'
        ]
ConsentDetailsColNames = {
        'B1_ALT_ID' : 'ConsentNo',
        'B1_APPL_STATUS' : 'ConsentStatus'
        }
ConsentDetailsImportFilter = {
       'B1_PER_SUB_TYPE' : ['Water Permit (s14)'] ,
       'B1_APPL_STATUS' : ConsentStatus     
        }
ConsentDetailswhere_op = 'AND'
ConsentDetailsDate_col = 'toDate'
ConsentDetailsfrom_date = EarliestTermination
ConsentDetailsServer = 'SQL2012Prod03'
ConsentDetailsDatabase = 'DataWarehouse'
ConsentDetailsTable = 'F_ACC_PERMIT'

ConsentDetails = pdsql.mssql.rd_sql(
                   server = ConsentDetailsServer,
                   database = ConsentDetailsDatabase, 
                   table = ConsentDetailsTable,
                   col_names = ConsentDetailsCol,
                   where_op = ConsentDetailswhere_op,
                   where_in =ConsentDetailsImportFilter,
                   date_col = ConsentDetailsDate_col,
                   from_date = ConsentDetailsfrom_date
                   )

# Format data
ConsentDetails.rename(columns=ConsentDetailsColNames, inplace=True)
ConsentDetails['ConsentNo'] = ConsentDetails['ConsentNo'].str.strip().str.upper()

# Remove consents issued after FY28/19
ConsentInfo = ConsentDetails[ConsentDetails['fmDate'] < LatestGivenEffect]

# Create list of consents active in this financial year
ConsentMaster = list(set(ConsentDetails['ConsentNo'].values.tolist()))


##############################################################################
### Import WAP Information


WAPDetailsCol = [
        'WAP',
        'RecordNumber',
        'Activity'     
        ]
WAPDetailsColNames = {
        'RecordNumber' : 'ConsentNo'
        }
WAPDetailsImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNumber' : ConsentMaster
        }
WAPDetailsWhere_op = 'AND'
WAPDetailsServer = 'SQL2012Prod03'
WAPDetailsDatabase = 'DataWarehouse'
WAPDetailsTable = 'D_ACC_Act_Water_TakeWaterWAPAllocation'

WAPDetails = pdsql.mssql.rd_sql(
                   server = WAPDetailsServer,
                   database = WAPDetailsDatabase, 
                   table = WAPDetailsTable,
                   col_names = WAPDetailsCol,
                   where_op = WAPDetailsWhere_op,
                   where_in = WAPDetailsImportFilter
                   )

# Format data
WAPDetails.rename(columns=WAPDetailsColNames, inplace=True)
WAPDetails['ConsentNo'] = WAPDetails['ConsentNo'].str.strip().str.upper()
WAPDetails['Activity'] = WAPDetails['Activity'].str.strip().str.lower()
WAPDetails['WAP'] = WAPDetails['WAP'].str.strip().str.upper()


##############################################################################
### Create Assoications Table
##############################################################################

Associations = pd.merge(ConsentDetails, WAPDetails, on = 'ConsentNo', how = 'left')

temp = Associations[['ConsentNo','WAP']]
temp = temp[temp['WAP'].notnull()]
AW1 = temp.copy()
AW2 = temp.copy()
AW2.columns= ['AssociatedConsent', 'WAP']
df = pd.merge(AW1, AW2, on = 'WAP', how = 'left')
df= df[['ConsentNo','AssociatedConsent']]
df = df.drop_duplicates()


##############################################################################
### Calculate groups of associated consents
##############################################################################

df.columns= ['ConsentNo', 'AssociatedConsent']
G = nx.from_pandas_edgelist(df, 'ConsentNo','AssociatedConsent')
subgraphs = list(nx.connected_components(G))
grouplist = list(range(len(subgraphs)))

def defineGrouping(x):
    return grouplist[[n for n,i in enumerate(subgraphs) if x in i][0]]

df['GroupNo'] = df.AssociatedConsent.map(defineGrouping)

df= df[['ConsentNo','GroupNo']]
df = df.drop_duplicates()

dfcount = df.groupby(
  ['GroupNo'], as_index=False
  ).agg(
          {
          'ConsentNo' : 'count'
          })
    
dfcount.columns= ['GroupNo', 'GroupSize']
dfcount = dfcount.drop_duplicates()  
  
### Number of groups
df.GroupNo.max()

### largest group
dfcount.GroupSize.max()


##############################################################################
### Join associations to supporting information
##############################################################################

df = pd.merge(df, dfcount, on = 'GroupNo', how = 'left')

AssociatedConsents = pd.merge(df, Associations, on = 'ConsentNo', how = 'left')

AssociatedConsents = AssociatedConsents[[
        'GroupNo',
        'GroupSize',
        'ConsentNo',
        'Activity',        
        'WAP',
        'HolderAddressFullName',   
        'HolderEcanID',        
        'ConsentStatus',
        'fmDate',
        'toDate'
        ]]

##############################################################################
### Output results
##############################################################################

AssociatedConsents.to_csv(os.path.join(output_path, 
                                       'AssociatedConsents-WAP_' + 
                                       RunDate + '.csv'))