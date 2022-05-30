#!/usr/bin/python

import pymssql
import os

os.environ["FREETDSCONF"] = "/etc/freetds.conf"

host = "msk00-syb01:5000"
user = "sa"
password = os.popen("/usr/bin/pass PROD_SYB01/%s" % user).read().rstrip()
base = "rafael"

conn = pymssql.connect(host = host, user = user, password = password, database = base, conn_properties = r"", autocommit=True)
curs = conn.cursor()

curs.execute("""
        select
                t1.M_KEY_ID,
                t1.M_SUM,
                t2.M_SUM,
                t3.M_SUM
        from (
                select trd.M_KEY_ID, sum(trd.M_QUANTITY) as M_SUM
                from MUREXDB.CONSDEAL#FXQORT#TRADES_DBF trd
                inner join MUREXDB.CONSDEAL#FXQORT#KEYS_DBF keys
                on trd.M_KEY_ID = keys.M_ID
                where keys.M_TRADE_DATE >= dateadd(dd, -1, current_date())
                and trd.M_EVENT_ID is not null
                group by trd.M_KEY_ID
        ) t1
        left join (
                select evt.M_KEY_ID, evt.M_QUANTITY as M_SUM
                from MUREXDB.CONSDEAL#FXQORT#EVENTS_DBF evt
                inner join MUREXDB.CONSDEAL#FXQORT#KEYS_DBF keys
                on evt.M_KEY_ID = keys.M_ID
                where keys.M_TRADE_DATE >= dateadd(dd, -1, current_date())
                and evt.M_STATUS_TAKEN = 'F'
                group by evt.M_KEY_ID
                having evt.M_ID = max(evt.M_ID)
        ) t2
        on t1.M_KEY_ID = t2.M_KEY_ID
        left join (
                select convert(numeric(10, 0), stc.M_OBJ_ALT) as M_KEY_ID, hdr.M_BRW_NOM2 as M_SUM
                from murex.MUREXDB.TRN_HDR_DBF hdr
                inner join murex.MUREXDB.CONTRACT_DBF con 
                on hdr.M_CONTRACT = con.M_REFERENCE 
                and hdr.M_OPT_STSVER = con.M_VERSION
                inner join murex.MUREXDB.TRN_EXT_DBF ext 
                on ext.M_TRADE_REF = hdr.M_NB 
                and ext.M_VERSION = con.M_VERSION
                inner join murex.MUREXDB.KEYMAP_STC_DBF stc 
                on stc.M_OBJ_ID = con.M_ORIG_REF
                where M_TRN_DATE >= dateadd(dd, -1, current_date())
                and M_SRC_MODULE = 1024
        ) t3
        on t1.M_KEY_ID = t3.M_KEY_ID
        where
                t1.M_SUM != t2.M_SUM
                or t1.M_SUM != t3.M_SUM
                or t2.M_SUM is null
                or t3.M_SUM is null
""")

res = curs.fetchall()

if len(res) != 0:
	print("ERROR")
	for item in res:
		print(item)
else:
	print("OK")
