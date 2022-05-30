#!/usr/bin/python

import os
import pymssql
import time
import datetime
import time


os.environ["FREETDSCONF"] = "/etc/freetds.conf"

user = r"sa"
password = os.popen('/usr/bin/pass PROD_SYB01/%s' % user).read().rstrip()
connMx = pymssql.connect(host = 'msk00-syb01:5000', user = user, password = password, database = r'rafael', conn_properties = r'',autocommit=True)
cursMx = connMx.cursor()
listMx = []

user = r"svcMurexWar"
password = os.popen('/usr/bin/pass QUIK/%s' % user).read().rstrip()
connQuik = pymssql.connect(host = 'quik-db:1433', user = user, password = password, database = r'QExport',autocommit=True)
cursQuik = connQuik.cursor()
listQuik = []


cursMx.execute("select M_EXCH_CODE from dbo.CONSDEAL#EQBROKER#TRADES_VIEW where M_TRADE_DATE=convert(varchar,current_date(),112) at isolation 0")
cursQuik.execute(''' 
					select ExchangeCode from dbo.Trades
					where convert(varchar, TradeDate, 112) = convert(varchar, getdate(), 112)
					and (
					ClassCode in (
						'HKEX_ALL',
						'LSE_SET_EXMP',
						'LSE_SET_SDGB',
						'LSE_SET_D',
						'CHIX',
						'LSE_IOB_D',
						'GW_LSE',
						'XETR',
						'BATS'
						)
					or 
					(
					substring(ClassCode,1,2)='NY' or
					substring(ClassCode,1,2)='NA' or
					substring(ClassCode,1,2)='HK' 
					))
					and BrokerRef not like 'RB331/DMC%'
							''')


def recheck(connMx, item):
    con = connMx.cursor()
    con.execute('''select M_EXCH_CODE from dbo.CONSDEAL#EQBROKER#TRADES_VIEW
                where M_EXCH_CODE = '%s'
                '''% (item))
    row = con.fetchone()
    result = row[0] if row else None 

    con.close()
    return result 



for itemMx in cursMx:
	listMx.append(itemMx[0])

for itemQk in cursQuik:
	listQuik.append(itemQk[0])

pre_result=list(set(listQuik) - set(listMx))
result = []	
time.sleep(2)#2 sec

if len(pre_result) != 0:
	for item in pre_result:
		trade = recheck(connMx, item)
		if trade:
			result.append(trade)
	if len(result) != 0:
		print("ERROR\n")
		print(len(result))
	else:
		print("OK\n")
else:
	print("OK\n")
	

print(r"/mrx/share/scripts/check_eqbroker_trades_diff.py")	
					
cursMx.close();
connMx.close();

cursQuik.close();
connQuik.close();

