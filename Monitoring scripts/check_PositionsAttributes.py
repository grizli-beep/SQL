#!/usr/bin/python

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

def nowTime():
    nowstrt = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    return nowstrt
pd.set_option('display.max_rows', None)

#SQL select 
mrxSelect = '''
select distinct TransactionType,Instrument,PlInstrument
from MR_PositionsAttributes 
where ReportDate>=convert(varchar,dateadd(dd,-1,getdate()),112) --and TransactionType like '%uture%' 
and isnull(Underlying,'')=''
and TransactionType not in('Cross Rate Swap','IR Swap','FRA')
            '''

#Variables
MxList = []
MxlistSecond = []
MxPortfolio = []
body = ''

ExctMX = pDBIconnect.dbi_connect_Michelangelo_mrxrecon(mrxSelect)
# for item in ExctMX:
#     MxList.append(str(item[0]))

for item in ExctMX:
    MxList.append(str(item[0]))
for item in ExctMX:
    MxlistSecond.append(str(item[1]))
for item in ExctMX:
    MxPortfolio.append(str(item[2]))

#Creating table through module Pandas
dataframe = pd.DataFrame({'TransactionType': MxList,
                   'Instrument': MxlistSecond, 
                   'PlInstrument': MxPortfolio,
                   })

if len(MxList) != 0:
    print ("\nStatus:CRITICAL\n")
    print(dataframe)
else:
    print ("OK\n")

if len(MxList) != 0:
    print("\nComment:\nPl's check setup of the instrument and GRDBMAP table ")

print("\nTIMESTAMP:\n" + nowTime())
print("/mrx/share/scripts/check_PositionsAttributes.py\n")



