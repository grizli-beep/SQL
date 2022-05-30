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

###SQL select###
mrxSelect_1 = '''
select
TR.Portfolio, TR.M_DSP_LABEL as Instrument, CONVERT(varchar,M_TRN_EXP,110) as Maturity, TR.M_NB, TR.SignedQty, TR.isInternal, TTL.TotalPos
from
(
select
case
when Dir.Direction ='S' then TRN.M_SPFOLIO
when Dir.Direction ='B' then TRN.M_BPFOLIO
else '' end as Portfolio,
M_TRN_EXP, M_DSP_LABEL, M_NB,
(
case
when Dir.Direction ='S' then -1.
when Dir.Direction ='B' then 1.
else 1000000. end
) * TRN.M_BRW_NOM1 as SignedQty,
case
when M_SINTERNAL = M_BINTERNAL then 'Yes'
else 'No' end as isInternal
from
MUREXDB.TRN_HDR_DBF TRN
left join
MUREXDB.TRN_PLIN_DBF PLIN
on
TRN.M_INSTRUMENT = convert(varchar,PLIN.M_ID)
left join
(
select 'S' as Direction
union all
select 'B' as Direction
) Dir
on
(TRN.M_SINTERNAL = 'Y' and Dir.Direction = 'S') or (M_BINTERNAL = 'Y' and Dir.Direction = 'B')
where
TRN.M_TRN_FMLY = 'IRD' and
TRN.M_TRN_GRP = 'BOND' and
not(TRN.M_TRN_STATUS = 'DEAD' and isNull(TRN.M_GXIT_DATE,'20100101') = '20100101') and
TRN.M_TRN_EXP > dateadd(dd,-3 ,current_date()) and TRN.M_TRN_EXP < dateadd(dd,0 ,current_date()) and
TRN.M_CNS_ACTIVE = 1
) TR
left join
(
select
Portfolio, M_DSP_LABEL, sum(SignedQty) as TotalPos
from
(
select
case
when Dir.Direction ='S' then TRN.M_SPFOLIO
when Dir.Direction ='B' then TRN.M_BPFOLIO
else '' end as Portfolio,
M_TRN_EXP, M_DSP_LABEL, M_NB,
(
case
when Dir.Direction ='S' then -1.
when Dir.Direction ='B' then 1.
else 1000000. end
) * TRN.M_BRW_NOM1 as SignedQty
from
MUREXDB.TRN_HDR_DBF TRN
left join
MUREXDB.TRN_PLIN_DBF PLIN
on
TRN.M_INSTRUMENT = convert(varchar,PLIN.M_ID)
left join
(
select 'S' as Direction
union all
select 'B' as Direction
) Dir
on
(TRN.M_SINTERNAL = 'Y' and Dir.Direction = 'S') or (M_BINTERNAL = 'Y' and Dir.Direction = 'B')
where
TRN.M_TRN_FMLY = 'IRD' and
TRN.M_TRN_GRP = 'BOND' and
not(TRN.M_TRN_STATUS = 'DEAD' and isNull(TRN.M_GXIT_DATE,'20100101') = '20100101') and
TRN.M_TRN_EXP > dateadd(dd,-3 ,current_date()) and TRN.M_TRN_EXP < dateadd(dd,0 ,current_date()) and
TRN.M_CNS_ACTIVE = 1
) Pos
group by
Portfolio, M_DSP_LABEL
) TTL
on
TR.Portfolio = TTL.Portfolio and TR.M_DSP_LABEL = TTL.M_DSP_LABEL
where
abs(isNull(TTL.TotalPos,1))>0.
order by
M_TRN_EXP, TR.M_DSP_LABEL, TR.Portfolio, TR.M_TRN_EXP, TR.M_NB
            '''

###Variables###
MxPortfolio = []
MxInstrument = []
MxMatuirty = []
MxContract = []
Mxinternal = []
MxTotalpos = []

###Insert from SQL in Variables####
ExctMX = pDBIconnect.dbi_connect_mrxProd(mrxSelect_1) 
for item in ExctMX:
    MxPortfolio.append(str(item[0]))
for item in ExctMX:
    MxInstrument.append(str(item[1]))
for item in ExctMX:
    MxMatuirty.append(str(item[2]))
for item in ExctMX:
    MxContract.append(str(item[3]))
for item in ExctMX:
    Mxinternal.append(str(item[5]))
for item in ExctMX:
    MxTotalpos.append(str(item[6]))

###Creating table through module Pandas###
dataframe = pd.DataFrame({'Portfolio': MxPortfolio, 
                   'Instrument': MxInstrument,
                   'Matuirty': MxMatuirty,
                   'Contract': MxContract,
                   'internal': Mxinternal,
                   })


###Conditions on Trigger###               
if len(MxPortfolio) != 0:
    print ("CRITICAL\n")
    print (dataframe)
else:
    print ("OK\n")

if len(MxPortfolio) != 0:
    print("\nComment:\nDescription on WIKI - check_bonds_expiration.py")

print("\nTIMESTAMP:\n" + nowTime())
print("Script: /mrx/share/scripts/check_bonds_expiration.py\n")


