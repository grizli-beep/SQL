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
pd.set_option('display.max_rows', 10)

def nowTime():
    nowstrt = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    return nowstrt

#SQL select 
mrxSelect = '''
select udf.M_LABEL,*  from MUREXDB.TRN_PFLD_DBF pfld  join MUREXDB.TABLE#DATA#PORTFOLI_DBF udf  on(pfld.M_LABEL=udf.M_LABEL)
where M_SUPERVISOR=''  and  M_IS_ALIVE='Yes' 
            '''

MxLabel = []


ExctMX = pDBIconnect.dbi_connect_mrxProd(mrxSelect) #WORK

for item in ExctMX:
    MxLabel.append(str(item[0]))


dataframe = pd.DataFrame({'': MxLabel})

if len(MxLabel) != 0:
    print ("CRITICAL\n")
    print (dataframe)
else:
    print ("OK\n")

print("\nTIMESTAMP:\n" + nowTime())
print("Script: /mrx/share/scripts/check_books_without_trader.py\n")