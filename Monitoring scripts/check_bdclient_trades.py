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


cursMx.execute("select M_TRADE_ID from dbo.CONSDEAL#BDCLIENT#TRADES_VIEW where M_TRADE_DATE=convert(varchar,current_date(),112) at isolation 0")
cursQuik.execute(''' 
					select TradeNum from Trades
					where ClassCode in ('PSOB','TQCB','TQOB') and ClientCode like '%DMC%'
				''')


def recheck(connMx, item):
    con = connMx.cursor()
    con.execute('''select M_TRADE_ID from dbo.CONSDEAL#BDCLIENT#TRADES_VIEW
                where M_TRADE_ID = '%s'
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
print("RESULT:\n")
time.sleep(2)#2 sec

if len(pre_result) != 0:
	for item in pre_result:
		trade = recheck(connMx, item)
		if trade:
			result.append(trade)
	if len(result) != 0:
		print("CRITICAL\n")
		for item in result:	
			print (item)
	else:
		print("OK\n")
else:
	print("OK\n")
	

print(r"/mrx/share/scripts/check_bdclient_trades.py")	
					
cursMx.close();
connMx.close();

cursQuik.close();
connQuik.close();

