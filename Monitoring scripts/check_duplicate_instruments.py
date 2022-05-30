#!/usr/bin/python3

#################
# Imports
import os
import time
import datetime
import time
from datetime import datetime as dt
from time import strftime
import pDBIconnect
import decimal
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def nowTime():
    nowstrt = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    return nowstrt

#SQL select 
mrxSelect = '''
select M_GRDB_ID,M_LABEL,CONVERT(varchar,M_MATURITY),M_CALLPUT, M_OPT_STYLE , M_OPT_DELIV,M_OPT_STRIKE,M_RTBS_TYPE, count(*)
from MUREXDB.SE_GRDBMAP_DBF  where M_GRP  in  ('EQUIT','INDEX','FXD','BOND')
group by M_GRDB_ID,M_LABEL
having  count(*)>1
union
select M_GRDB_ID,M_LABEL,CONVERT(varchar,M_MATURITY),M_CALLPUT, M_OPT_STYLE , M_OPT_DELIV,M_OPT_STRIKE,M_RTBS_TYPE, count(*) 
from MUREXDB.SE_GRDBMAP_DBF 
where M_MATURITY>=getdate()
group by M_LABEL,M_MATURITY,M_CALLPUT, M_OPT_STYLE , M_OPT_DELIV,M_OPT_STRIKE ,M_RTBS_TYPE
having  count(*)>1

            '''

MxGRDB = []
MxLabel = []
MxMat = []
MxCP = []
MxSt = []
MxDeliv=[]
MxStrike=[]
MxRtbs=[]
MxCount=[]

ExctMX = pDBIconnect.dbi_connect_mrxProd(mrxSelect) #WORK


for item in ExctMX:
    MxGRDB.append(int(item[0]))
for item in ExctMX:
    MxLabel.append(str(item[1]))
for item in ExctMX:
    MxMat.append((item[2]))
for item in ExctMX:
    MxCP.append((item[3]))
for item in ExctMX:
    MxSt.append((item[4]))
for item in ExctMX:
    MxDeliv.append((item[5]))  
for item in ExctMX:
    MxStrike.append(int(item[6]))          
for item in ExctMX:
    MxRtbs.append((item[7])) 
for item in ExctMX:
    MxCount.append(int(item[8])) 


dataframe = pd.DataFrame({'GRDBID': MxGRDB, 
                   'Label': MxLabel,
                   'Maturity': MxMat,
                   'Call/Put':MxCP,
                   'OPT_Style':MxSt,
                   'OPT_Deliv':MxDeliv,
                   'Strike':MxStrike,
                   'RTBS_Type':MxRtbs,
                   'Count':MxCount,
                   



                   })

if len(MxLabel) != 0:
    print ("CRITICAL\n")
    print (dataframe)
else:
    print ("OK\n")

print("\nTIMESTAMP:\n" + nowTime())
print("Script: /mrx/share/scripts/check_duplicate_instruments.py\n")