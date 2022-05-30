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

def nowTime():
    nowstrt = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    return nowstrt

#SQL select 
mrxSelect_1 = '''
select 'ID=' + CONVERT(nvarchar, M_CONTRACT_ID) + ' : DIFF_1=' +  STR(Dif1,6,1) + ' : DIFF_2=' + STR(Dif2,6,1) as INFO from
(select  M_CONTRACT_ID,M_TRADE_DATE,M_TIMESTAMP,M_KEY_ID,
case 
when M_BRW_NOMU1 = QCurr then abs(abs(M_BRW_NOM1)-abs(M_QUANTITY))
else abs(abs(M_BRW_NOM2)-abs(M_QUANTITY)) end as Dif1,
case 
when M_BRW_NOMU1 = QCurr then abs(abs(M_BRW_NOM2)-abs(M_OTHER_QUANTITY))
else abs(abs(M_BRW_NOM1)-abs(M_OTHER_QUANTITY)) end as Dif2
from 
(select P.M_TIMESTAMP,P.M_INSTRUMENT,M_BRW_NOM1,M_BRW_NOMU1,M_BRW_NOM2,M_BRW_NOMU2,
M_QUANTITY,Substring(P.M_INSTRUMENT,1,3)as QCurr, 
M_OTHER_QUANTITY,Substring(P.M_INSTRUMENT,5,3) as OQCurr, 
abs(abs(M_OTHER_QUANTITY)-abs(M_BRW_NOM2)),
abs(abs(M_OTHER_QUANTITY)-abs(M_BRW_NOM1)),M_CONTRACT_ID,
M_KEY_ID,M_TRADE_DATE  from rafael.dbo.CONSDEAL#FXIB#POSITION_VIEW P
left join MUREXDB.CONTRACT_DBF C on P.M_CONTRACT_ID = C.M_ORIG_REF
join MUREXDB.TRN_HDR_DBF T on T.M_CONTRACT = C.M_REFERENCE and T.M_OPT_STSVER = C.M_VERSION
join MUREXDB.TRN_EXT_DBF EXT on 
EXT.M_TRADE_REF=T.M_NB and EXT.M_VERSION=C.M_VERSION 
where  not((EXT.M_EVT_INTID="1.220" and M_ACTION <>8) or 
(EXT.M_EVT_INTID="MwDcj67841" and M_ACTION =8) or (EXT.M_EVT_INTID="MoBbP64477" and EXT.M_CNL_STS=2)  )
and M_TRADE_DATE >= dateadd(day,-5,current_date())) as T--and M_INTERBOOK_TYPE ='fidessa' 
)as T2
where (Dif1>0.001 or  Dif2>0.001) and M_TIMESTAMP < dateadd(minute,-20,getDate())
            '''

#Variables
MxlistContract = []

ExctMX = pDBIconnect.dbi_connect_mrxProd(mrxSelect_1) #WORK
for item in ExctMX:
    MxlistContract.append(str(item[0]))

if len(MxlistContract) != 0:
    bodyWork = "\n" . join(str(item) for item in MxlistContract[0:100])
    body = "Deal(s):\n" + bodyWork + "\n"
    print ("CRITICAL\n" + body)
else:
    print ("OK\n")

if len(MxlistContract) != 0:
    print("Comment:\nNeccesary to do empty C&R and reprocess deal from EVENTS through Resend OSP")

print("\nTIMESTAMP:\n" + nowTime())
print("Script: /mrx/share/scripts/check_FXIB_trade_amount.py\n")


