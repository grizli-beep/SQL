#!/usr/bin/python3

import os
import pymssql
import time
import datetime
import time

os.environ["FREETDSCONF"] = "/etc/freetds.conf"

user = r"sa"
password = os.popen('/usr/bin/pass PROD_SYB01/%s' % user).read().rstrip()
connMx = pymssql.connect(host = 'msk00-syb01:5000', user = user, password = password, database = r'murex', conn_properties = r'',autocommit=True)
cursMx = connMx.cursor(as_dict=True)
listMx = []



cursMx.execute('''select RES.* from
				(
				select  
						T11.M_CONTRACT MX_CNT, 
						case when M_BPFOLIO='EWEQRC' or M_BPFOLIO='EWEQRC' then T11.M_BRW_NOM1 else -1*T11.M_BRW_NOM1 end as MX_QTY,
						case when M_BPFOLIO='EWEQRC' or M_BPFOLIO='EWEQRC' then T11.M_BRW_NOM2 else -1*T11.M_BRW_NOM2 end as MX_QTY2,
						T11.M_GID as GID,
						isnull((select sum(isnull(BRK.M_FEE,0)) from murex.MUREXDB.TRN_BROKER_DBF BRK 
						where BRK.M_NB=T11.M_NB and M_REFERENCE in 
						(select max(M_REFERENCE) from murex.MUREXDB.TRN_BROKER_DBF
						where M_NB=T11.M_NB
						group by M_LINE)
						),0) as MX_FEE
				from 
						murex.MUREXDB.TRN_HDR_DBF T11
				inner join murex.MUREXDB.CONTRACT_DBF T2 
						on T11.M_CONTRACT = T2.M_REFERENCE 
						and T2.M_VERSION = T11.M_OPT_STSVER 
				inner join murex.MUREXDB.TRN_EXT_DBF EXT 
						on EXT.M_TRADE_REF=T11.M_NB 
						and EXT.M_VERSION=T2.M_VERSION
				left join murex.MUREXDB.KEYMAP_STC_DBF t3
						on t3.M_OBJ_ID = T2.M_ORIG_REF
				where T11.M_GID like 'FORTSCLIENT%'
				and M_TRN_STATUS = 'LIVE'
				and convert(varchar,M_TRN_DATE,112) = convert(varchar,getdate(),112)
				) RES
				group by RES.GID
				having sum(RES.MX_QTY) != 0
					or sum(RES.MX_QTY2) >= 0.0001 
					or sum(RES.MX_FEE) != 0
				''')



for itemMx in cursMx:
	listMx.append(itemMx['GID'])



if len(listMx) != 0:
	print("CRITICAL\n")
	for item in listMx:	
			print (item)
else:
	print("OK\n")
	

print(r"/mrx/share/scripts/check_fxclient_keys.py")	
					
cursMx.close();
connMx.close();

